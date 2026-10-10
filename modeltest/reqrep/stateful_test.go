// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reqrep

// Stateful property tests derived from models/RequestReply. Each rule is one
// of the model's actions (or a short, settled sequence of them) performed
// against the real client; the machine invariants are the model's
// invariants evaluated on the real connection after every step:
//
//   rule                 model actions
//   request              NewCreate, NewPublishOk, Deliver, HandlerLookup,
//                        HandlerSend, NewRecv (or OldSubscribe..OldUnsub)
//   request_dup          ... with MaxReplies = 2 (a duplicate reply)
//   request_noresp       Deliver(t, "503", TRUE) -> ErrNoResponders
//   request_timeout      NewTimeout, NewTimeoutDelete / OldRecv(timeout)
//   late_reply           Deliver for a token whose request already returned
//   burst                interleaved requesters + late replies
//   cut                  Disconnect, Reconnect
//   close_inflight       Close while requests wait -> NewRecvClosed
//   request_pub_error    NewPublishErr                  (REQ-1, gated)
//   unsub_reply_sub      UserUnsubMux                   (REQ-2, gated)
//
//   invariant            TLA+ invariant
//   no_leak              NoLeak
//   mux_valid            MuxValid
//   one_mux              MuxCreatedOnce (the new-style conn holds exactly
//                        the mux subscription, old style holds none)

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/modeltest/harness"
	"github.com/wallyqs/zizek.go"
)

// env is shared by all examples of one test: a server, a proxy for the
// clients under test, and a responder connection that bypasses the proxy.
type env struct {
	srv   *harness.Server
	proxy *harness.Proxy
	resp  *nats.Conn

	mu     sync.Mutex
	silent []string // reply subjects of requests the silent responder swallowed
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{srv: harness.RunServer(t, false)}
	e.proxy = harness.NewProxy(t, e.srv.Addr())
	e.resp = connect(t, e.srv.URL())
	sub := func(subj string, cb nats.MsgHandler) {
		if _, err := e.resp.Subscribe(subj, cb); err != nil {
			t.Fatal(err)
		}
	}
	sub("svc.echo", func(m *nats.Msg) { _ = m.Respond(m.Data) })
	sub("svc.dup", func(m *nats.Msg) {
		_ = m.Respond(m.Data)
		_ = m.Respond(append([]byte("dup-"), m.Data...))
	})
	sub("svc.silent", func(m *nats.Msg) {
		e.mu.Lock()
		e.silent = append(e.silent, m.Reply)
		e.mu.Unlock()
	})
	if err := e.resp.Flush(); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) silentCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.silent)
}

// client is one connection under test.
type client struct {
	name        string
	old         bool
	nc          *nats.Conn
	reconnected chan struct{}
}

func (e *env) dial(name string, old bool) (*client, error) {
	c := &client{name: name, old: old, reconnected: make(chan struct{}, 64)}
	opts := []nats.Option{
		nats.ReconnectWait(10 * time.Millisecond),
		nats.ReconnectJitter(0, 0),
		nats.MaxReconnects(-1),
		nats.ReconnectHandler(func(*nats.Conn) { c.reconnected <- struct{}{} }),
	}
	if old {
		opts = append(opts, nats.UseOldRequestStyle())
	}
	nc, err := nats.Connect(e.proxy.URL(), opts...)
	if err != nil {
		return nil, err
	}
	c.nc = nc
	return c, nil
}

type machineState struct {
	e       *env
	clients [2]*client // [0] new style, [1] old style
	seq     atomic.Int64
}

// ruleCounts is a debugging aid: set REQREP_COUNT=1 to log how often each
// rule ran.
var (
	ruleCountsMu sync.Mutex
	ruleCounts   map[string]int
)

func count(name string) {
	if ruleCounts != nil {
		ruleCountsMu.Lock()
		ruleCounts[name]++
		ruleCountsMu.Unlock()
	}
}

func (s *machineState) payload(tag string) []byte {
	return []byte(fmt.Sprintf("%s-%d", tag, s.seq.Add(1)))
}

func (s *machineState) pickClient(tc *zizek.TestCase) (*client, error) {
	i, err := tc.Integer(0, 1, 0)
	if err != nil {
		return nil, err
	}
	return s.clients[i], nil
}

// api is the request entry point: Request, RequestMsg or RequestWithContext.
type api int

const (
	apiRequest api = iota
	apiRequestMsg
	apiWithContext
)

func (a api) String() string {
	return [...]string{"Request", "RequestMsg", "RequestWithContext"}[a]
}

func do(nc *nats.Conn, a api, subj string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	switch a {
	case apiRequestMsg:
		return nc.RequestMsg(&nats.Msg{Subject: subj, Data: data}, timeout)
	case apiWithContext:
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return nc.RequestWithContext(ctx, subj, data)
	default:
		return nc.Request(subj, data, timeout)
	}
}

func isTimeout(err error) bool {
	return errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded)
}

func drawAPI(tc *zizek.TestCase) (api, error) {
	i, err := tc.Integer(0, 2, 0)
	return api(i), err
}

// --- rules -----------------------------------------------------------------

func (s *machineState) request(tc *zizek.TestCase) error {
	c, err := s.pickClient(tc)
	if err != nil {
		return err
	}
	a, err := drawAPI(tc)
	if err != nil {
		return err
	}
	data := s.payload("echo")
	tc.Note(fmt.Sprintf("%s %s svc.echo %s", c.name, a, data))
	m, err := do(c.nc, a, "svc.echo", data, 5*time.Second)
	if err != nil {
		return fmt.Errorf("%s %s: %v", c.name, a, err)
	}
	if string(m.Data) != string(data) {
		return fmt.Errorf("NoMisdelivery: %s asked %q, got %q", c.name, data, m.Data)
	}
	return nil
}

func (s *machineState) requestDup(tc *zizek.TestCase) error {
	c, err := s.pickClient(tc)
	if err != nil {
		return err
	}
	data := s.payload("dup")
	tc.Note(fmt.Sprintf("%s Request svc.dup %s", c.name, data))
	m, err := c.nc.Request("svc.dup", data, 5*time.Second)
	if err != nil {
		return fmt.Errorf("%s dup: %v", c.name, err)
	}
	if string(m.Data) != string(data) {
		return fmt.Errorf("NoMisdelivery: %s asked %q, got %q", c.name, data, m.Data)
	}
	// Let the duplicate arrive (and be dropped) before the invariants run.
	return c.nc.Flush()
}

func (s *machineState) requestNoResponders(tc *zizek.TestCase) error {
	c, err := s.pickClient(tc)
	if err != nil {
		return err
	}
	a, err := drawAPI(tc)
	if err != nil {
		return err
	}
	tc.Note(fmt.Sprintf("%s %s svc.none", c.name, a))
	if _, err := do(c.nc, a, "svc.none", nil, 5*time.Second); !errors.Is(err, nats.ErrNoResponders) {
		return fmt.Errorf("%s %s to no responders: err = %v, want ErrNoResponders", c.name, a, err)
	}
	return nil
}

func (s *machineState) requestTimeout(tc *zizek.TestCase) error {
	c, err := s.pickClient(tc)
	if err != nil {
		return err
	}
	a, err := drawAPI(tc)
	if err != nil {
		return err
	}
	tc.Note(fmt.Sprintf("%s %s svc.silent (times out)", c.name, a))
	before := s.e.silentCount()
	if _, err := do(c.nc, a, "svc.silent", nil, 30*time.Millisecond); !isTimeout(err) {
		return fmt.Errorf("%s %s to a silent responder: err = %v, want a timeout", c.name, a, err)
	}
	// Make sure the request reached the responder so late_reply can use it.
	harness.Eventually(2*time.Second, func() bool { return s.e.silentCount() > before })
	return nil
}

func (s *machineState) lateReply(tc *zizek.TestCase) error {
	n := s.e.silentCount()
	tc.Assume(n > 0)
	i, err := tc.Integer(0, int64(n-1), 0)
	if err != nil {
		return err
	}
	s.e.mu.Lock()
	reply := s.e.silent[i]
	s.e.mu.Unlock()
	tc.Note(fmt.Sprintf("late reply #%d", i))
	if err := s.e.resp.Publish(reply, []byte("late")); err != nil {
		return err
	}
	if err := s.e.resp.Flush(); err != nil {
		return err
	}
	// Round trip on both clients so the late reply has been processed.
	for _, c := range s.clients {
		if err := c.nc.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// burst runs several requesters at once on both connections, interleaved
// with late replies, and checks every echo against its own payload.
func (s *machineState) burst(tc *zizek.TestCase) error {
	k, err := tc.Integer(2, 8, 2)
	if err != nil {
		return err
	}
	type job struct {
		c      *client
		a      api
		silent bool
	}
	jobs := make([]job, k)
	for i := range jobs {
		c, err := s.pickClient(tc)
		if err != nil {
			return err
		}
		a, err := drawAPI(tc)
		if err != nil {
			return err
		}
		silent, err := tc.Boolean()
		if err != nil {
			return err
		}
		jobs[i] = job{c, a, silent}
	}
	lates, err := tc.Integer(0, 3, 0)
	if err != nil {
		return err
	}
	tc.Note(fmt.Sprintf("burst of %d requests with %d late replies", k, lates))
	s.e.mu.Lock()
	late := append([]string(nil), s.e.silent...)
	s.e.mu.Unlock()

	errs := make(chan error, len(jobs))
	var wg sync.WaitGroup
	for _, j := range jobs {
		data := s.payload("burst")
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			if j.silent {
				if _, err := do(j.c.nc, j.a, "svc.silent", data, 30*time.Millisecond); !isTimeout(err) {
					errs <- fmt.Errorf("%s %s silent: err = %v, want a timeout", j.c.name, j.a, err)
				}
				return
			}
			m, err := do(j.c.nc, j.a, "svc.echo", data, 5*time.Second)
			if err != nil {
				errs <- fmt.Errorf("%s %s echo: %v", j.c.name, j.a, err)
			} else if string(m.Data) != string(data) {
				errs <- fmt.Errorf("NoMisdelivery: %s asked %q, got %q", j.c.name, data, m.Data)
			}
		}(j)
	}
	for i := 0; i < int(lates) && len(late) > 0; i++ {
		_ = s.e.resp.Publish(late[(i*7)%len(late)], []byte("late"))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	for _, c := range s.clients {
		if err := c.nc.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (s *machineState) cut(tc *zizek.TestCase) error {
	tc.Note("cut the client connections")
	for _, c := range s.clients {
		for len(c.reconnected) > 0 {
			<-c.reconnected
		}
	}
	s.e.proxy.Cut()
	for _, c := range s.clients {
		select {
		case <-c.reconnected:
		case <-time.After(10 * time.Second):
			return fmt.Errorf("%s did not reconnect", c.name)
		}
	}
	return nil
}

// closeInflight parks requests on the silent responder, closes the
// connection and checks CloseReleasesWaiters, then dials a fresh connection
// so the run can go on.
func (s *machineState) closeInflight(tc *zizek.TestCase) error {
	ci, err := tc.Integer(0, 1, 0)
	if err != nil {
		return err
	}
	c := s.clients[ci]
	k, err := tc.Integer(1, 4, 1)
	if err != nil {
		return err
	}
	tc.Note(fmt.Sprintf("close %s with %d requests in flight", c.name, k))
	before := s.e.silentCount()
	done := make(chan error, k)
	for i := 0; i < int(k); i++ {
		a := api(i % 3)
		go func() {
			_, err := do(c.nc, a, "svc.silent", nil, time.Minute)
			done <- err
		}()
	}
	if !harness.Eventually(5*time.Second, func() bool { return s.e.silentCount() >= before+int(k) }) {
		return fmt.Errorf("requests did not reach the responder")
	}
	c.nc.Close()
	for i := 0; i < int(k); i++ {
		select {
		case err := <-done:
			if !errors.Is(err, nats.ErrConnectionClosed) {
				return fmt.Errorf("CloseReleasesWaiters: in-flight request on %s returned %v, want ErrConnectionClosed", c.name, err)
			}
		case <-time.After(2 * time.Second):
			return fmt.Errorf("CloseReleasesWaiters: in-flight request on %s still blocked 2s after Close", c.name)
		}
	}
	if _, err := c.nc.Request("svc.echo", nil, time.Second); !errors.Is(err, nats.ErrConnectionClosed) {
		return fmt.Errorf("request on closed %s: err = %v, want ErrConnectionClosed", c.name, err)
	}
	nc, err := s.e.dial(c.name, c.old)
	if err != nil {
		return err
	}
	s.clients[ci] = nc
	return nil
}

// REQ-1 rule: a request whose publish fails.
func (s *machineState) requestPubError(tc *zizek.TestCase) error {
	c, err := s.pickClient(tc)
	if err != nil {
		return err
	}
	a, err := drawAPI(tc)
	if err != nil {
		return err
	}
	tc.Note(fmt.Sprintf("%s %s to an invalid subject", c.name, a))
	if _, err := do(c.nc, a, "bad subject", nil, time.Second); !errors.Is(err, nats.ErrBadSubject) {
		return fmt.Errorf("%s %s bad subject: err = %v", c.name, a, err)
	}
	return nil
}

// REQ-2 rule: the caller unsubscribes the Sub of a reply it received.
func (s *machineState) unsubReplySub(tc *zizek.TestCase) error {
	c, err := s.pickClient(tc)
	if err != nil {
		return err
	}
	tc.Note(fmt.Sprintf("%s: request, then reply.Sub.Unsubscribe()", c.name))
	m, err := c.nc.Request("svc.echo", s.payload("unsub"), 5*time.Second)
	if err != nil {
		return fmt.Errorf("%s: %v", c.name, err)
	}
	err = m.Sub.Unsubscribe()
	if c.old && !errors.Is(err, nats.ErrBadSubscription) {
		// Old style: the per-request sub is already gone.
		return fmt.Errorf("old-style reply sub unsubscribe: %v", err)
	}
	return nil
}

// --- invariants ------------------------------------------------------------

func (s *machineState) noLeak(_ *zizek.TestCase) error {
	for _, c := range s.clients {
		if n := Peek(c.nc).RespMapLen; n != 0 {
			return fmt.Errorf("NoLeak: %s respMap holds %d entries with no request in flight", c.name, n)
		}
	}
	return nil
}

func (s *machineState) muxValid(_ *zizek.TestCase) error {
	for _, c := range s.clients {
		if st := Peek(c.nc); st.HasMux && !st.MuxValid {
			return fmt.Errorf("MuxValid: %s nc.respMux is set but its subscription is closed", c.name)
		}
	}
	return nil
}

func (s *machineState) oneMux(_ *zizek.TestCase) error {
	for _, c := range s.clients {
		want := 0
		if !c.old && Peek(c.nc).HasMux {
			want = 1
		}
		if n := c.nc.NumSubscriptions(); n != want {
			return fmt.Errorf("MuxCreatedOnce: %s holds %d subscriptions, want %d", c.name, n, want)
		}
	}
	return nil
}

func runMachine(t *testing.T, bugRules []zizek.Rule[machineState], cfg zizek.Config) {
	e := newEnv(t)
	property := func(tc *zizek.TestCase) error {
		s := &machineState{e: e}
		var err error
		if s.clients[0], err = e.dial("new", false); err != nil {
			return err
		}
		defer func() { s.clients[0].nc.Close() }()
		if s.clients[1], err = e.dial("old", true); err != nil {
			return err
		}
		defer func() { s.clients[1].nc.Close() }()
		rules := []zizek.Rule[machineState]{
			{Name: "request", Apply: (*machineState).request},
			{Name: "request_dup", Apply: (*machineState).requestDup},
			{Name: "request_noresp", Apply: (*machineState).requestNoResponders},
			{Name: "request_timeout", Apply: (*machineState).requestTimeout},
			{Name: "late_reply", Apply: (*machineState).lateReply},
			{Name: "burst", Apply: (*machineState).burst},
			{Name: "cut", Apply: (*machineState).cut},
			{Name: "close_inflight", Apply: (*machineState).closeInflight},
		}
		rules = append(rules, bugRules...)
		for i := range rules {
			name, apply := rules[i].Name, rules[i].Apply
			rules[i].Apply = func(s *machineState, tc *zizek.TestCase) error {
				err := apply(s, tc)
				if err == nil {
					count(name)
				}
				return err
			}
		}
		machine := zizek.Machine[machineState]{
			Rules: rules,
			Invariants: []zizek.Rule[machineState]{
				{Name: "no_leak", Apply: (*machineState).noLeak},
				{Name: "mux_valid", Apply: (*machineState).muxValid},
				{Name: "one_mux", Apply: (*machineState).oneMux},
			},
		}
		err = zizek.RunSteps(s, machine, tc, 20)
		if ruleCounts != nil {
			ruleCountsMu.Lock()
			ruleCounts["examples"]++
			ruleCountsMu.Unlock()
		}
		return err
	}
	if os.Getenv("REQREP_COUNT") == "1" {
		ruleCounts = map[string]int{}
		defer func() { t.Logf("rule counts: %v", ruleCounts); ruleCounts = nil }()
	}
	if err := zizek.Check(property, cfg); err != nil {
		t.Fatal(err)
	}
}

// The model's actions without the two known-bug triggers: every invariant
// must hold.
func TestStatefulRequestReply(t *testing.T) {
	runMachine(t, nil, zizek.Config{Examples: 200, Seed: 7, SuppressTooSlow: true})
}

// Adds the REQ-1 trigger (NewPublishErr). Fails today; zizek shrinks the run
// to the single step that breaks NoLeak.
func TestStatefulRequestReplyREQ1(t *testing.T) {
	harness.KnownBug(t, "REQ-1")
	runMachine(t, []zizek.Rule[machineState]{
		{Name: "request_pub_error", Apply: (*machineState).requestPubError},
	}, zizek.Config{Examples: 200, Seed: 7, SuppressTooSlow: true})
}

// Adds the REQ-2 trigger (UserUnsubMux). Fails today; zizek shrinks the run
// to the single step that breaks MuxValid.
func TestStatefulRequestReplyREQ2(t *testing.T) {
	harness.KnownBug(t, "REQ-2")
	runMachine(t, []zizek.Rule[machineState]{
		{Name: "unsub_reply_sub", Apply: (*machineState).unsubReplySub},
	}, zizek.Config{Examples: 200, Seed: 7, SuppressTooSlow: true})
}
