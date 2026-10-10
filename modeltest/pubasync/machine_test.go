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

package pubasync

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/modeltest/harness"
	"github.com/wallyqs/zizek.go"
)

// The stateful tests drive the real publisher through the actions of
// models/PublishAsync and check the model's invariants after every step.
// The server side of every publish is a scripted responder (a plain
// subscriber on ctl.<run>.>) so that *when* and *how* each publish is
// answered is a rule the machine draws -- Respond(c, "ok"|"err"|"nr") in
// the model -- rather than a race. Every rule waits for the effects it
// causes to settle before the invariants run, so a step's outcome is
// determined by the drawn sequence alone.

const (
	machineMaxPA      = 2
	machineRetries    = 2
	machineRetryWait  = 2 * time.Millisecond
	machineStallWait  = 20 * time.Millisecond
	settle            = 2 * time.Second
	stalledStallWait  = 3 * time.Second
	stalledWakeBudget = time.Second
)

type futState int

const (
	pending futState = iota
	acked
	failed
)

// fut pairs a real PubAckFuture with the model's view of it.
type fut struct {
	subj    string
	paf     jetstream.PubAckFuture
	state   futState
	wantErr error // for failed futures: errors.Is target (nil: any *APIError)
	retries int   // 503s consumed (paf.retries)
	// observed resolution, consumed from Ok()/Err() exactly once
	seenAck *jetstream.PubAck
	seenErr error
}

// responder plays the stream: it records each publish's reply subject and
// answers only when a rule tells it to.
type responder struct {
	mu   sync.Mutex
	reqs map[string][]string // subject -> reply subjects, in arrival order
	side *nats.Conn
	seq  atomic.Uint64
}

func (r *responder) handle(m *nats.Msg) {
	r.mu.Lock()
	r.reqs[m.Subject] = append(r.reqs[m.Subject], m.Reply)
	r.mu.Unlock()
}

func (r *responder) count(subj string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reqs[subj])
}

func (r *responder) last(subj string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	reqs := r.reqs[subj]
	return reqs[len(reqs)-1]
}

func (r *responder) reply(subj, kind string) error {
	m := nats.NewMsg(r.last(subj))
	switch kind {
	case "ok":
		m.Data = []byte(fmt.Sprintf(`{"stream":"S","seq":%d}`, r.seq.Add(1)))
	case "err":
		m.Data = []byte(`{"error":{"code":400,"err_code":10060,"description":"expected stream does not match"}}`)
	case "nr":
		m.Header.Set("Status", "503") // what handleAsyncReply checks (publish.go:501)
	}
	if err := r.side.PublishMsg(m); err != nil {
		return err
	}
	return r.side.Flush()
}

// completeCh is a channel returned by PublishAsyncComplete. Once the model's
// pending count has reached zero it must be (and stay) closed, even if new
// publishes are pending again later; until then it must be open.
type completeCh struct {
	ch        <-chan struct{}
	mustClose bool
}

// drained marks every outstanding Complete channel as due once nothing is
// pending in the model.
func (m *pubMachine) drained() {
	if len(m.pending()) == 0 {
		for _, c := range m.complete {
			c.mustClose = true
		}
	}
}

// stalled is a PublishAsync parked in registerPAF's max-pending stall.
type stalled struct {
	subj    string
	started time.Time
	done    chan struct{}
	paf     jetstream.PubAckFuture
	err     error
}

type pubMachine struct {
	tc       *zizek.TestCase
	px       *harness.Proxy
	nc       *nats.Conn
	js       jetstream.JetStream
	resp     *responder
	run      int64
	next     int
	futs     *zizek.Pool[*fut]
	all      []*fut
	errCb    map[string][]error
	ackCb    map[string]int
	cbMu     sync.Mutex
	complete []*completeCh
	stall    *stalled // at most one
	closed   bool
}

func (m *pubMachine) pending() []*fut {
	var out []*fut
	for _, f := range m.all {
		if f.state == pending {
			out = append(out, f)
		}
	}
	return out
}

func (m *pubMachine) subject() string {
	m.next++
	return fmt.Sprintf("ctl.%d.%d", m.run, m.next)
}

func (m *pubMachine) addFuture(subj string, paf jetstream.PubAckFuture) error {
	f := &fut{subj: subj, paf: paf}
	m.all = append(m.all, f)
	if !harness.Eventually(settle, func() bool { return m.resp.count(subj) == 1 }) {
		return fmt.Errorf("publish of %s never reached the server", subj)
	}
	return m.futs.Add(m.tc, f)
}

// failAll is the model's reconnect / cleanup / close: every pending future
// fails with err.
func (m *pubMachine) failAll(err error) {
	for _, f := range m.pending() {
		f.state, f.wantErr = failed, err
	}
}

// --- rules -----------------------------------------------------------------

// PubStart+PubReg+PubSend, or PubReg -> stall -> ErrTooManyStalledMsgs at the cap.
func (m *pubMachine) publish(tc *zizek.TestCase) error {
	tc.Assume(!m.closed && m.stall == nil)
	subj := m.subject()
	atCap := len(m.pending()) >= machineMaxPA
	paf, err := m.js.PublishAsync(subj, []byte("x"),
		jetstream.WithRetryAttempts(machineRetries),
		jetstream.WithRetryWait(machineRetryWait),
		jetstream.WithStallWait(machineStallWait))
	tc.Note(fmt.Sprintf("publish %s (atCap=%v) -> %v", subj, atCap, err))
	if atCap {
		if !errors.Is(err, jetstream.ErrTooManyStalledMsgs) {
			return fmt.Errorf("publish at max pending %d returned %v, want ErrTooManyStalledMsgs", machineMaxPA, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("publish below the cap failed: %v", err)
	}
	return m.addFuture(subj, paf)
}

func (m *pubMachine) drawPending(tc *zizek.TestCase) *fut {
	f, err := zizek.Draw(tc, m.futs.ValuesReusable())
	if err != nil {
		panic(err)
	}
	tc.Assume((*f).state == pending)
	return *f
}

// Respond(c, "ok"): handleAsyncReply's good-ack path.
func (m *pubMachine) ack(tc *zizek.TestCase) error {
	tc.Assume(!m.closed)
	f := m.drawPending(tc)
	tc.Note("ack " + f.subj)
	if err := m.resp.reply(f.subj, "ok"); err != nil {
		return err
	}
	f.state = acked
	return m.afterRelease()
}

// Respond(c, "err"): an API error ack.
func (m *pubMachine) errAck(tc *zizek.TestCase) error {
	tc.Assume(!m.closed)
	f := m.drawPending(tc)
	tc.Note("error ack " + f.subj)
	if err := m.resp.reply(f.subj, "err"); err != nil {
		return err
	}
	f.state, f.wantErr = failed, nil
	return m.afterRelease()
}

// Respond(c, "nr"): 503 -> retry (RtyFire/RtyGet/RtyPub) or ErrNoStreamResponse.
func (m *pubMachine) noResponders(tc *zizek.TestCase) error {
	tc.Assume(!m.closed)
	f := m.drawPending(tc)
	tc.Note(fmt.Sprintf("503 %s (retries so far %d)", f.subj, f.retries))
	if err := m.resp.reply(f.subj, "nr"); err != nil {
		return err
	}
	if f.retries < machineRetries {
		f.retries++
		if !harness.Eventually(settle, func() bool { return m.resp.count(f.subj) == f.retries+1 }) {
			return fmt.Errorf("503 retry of %s never republished", f.subj)
		}
		return nil
	}
	f.state, f.wantErr = failed, jetstream.ErrNoStreamResponse
	return m.afterRelease()
}

// A late / duplicate reply for an already resolved future must be ignored.
func (m *pubMachine) duplicate(tc *zizek.TestCase) error {
	tc.Assume(!m.closed)
	f, err := zizek.Draw(tc, m.futs.ValuesReusable())
	if err != nil {
		return err
	}
	tc.Assume((*f).state != pending)
	kind, err := zizek.Draw(tc, zizek.SampledFrom([]string{"ok", "err", "nr"}))
	if err != nil {
		return err
	}
	tc.Note(fmt.Sprintf("late %s reply for resolved %s", kind, (*f).subj))
	if err := m.resp.reply((*f).subj, kind); err != nil {
		return err
	}
	return m.resp.side.Flush()
}

// UserErrCall: create Ok()/Err() before resolution, so resolution goes
// through the channel send instead of the pre-load.
func (m *pubMachine) observe(tc *zizek.TestCase) error {
	f := m.drawPending(tc)
	tc.Note("observe " + f.subj)
	_, _ = f.paf.Ok(), f.paf.Err()
	return nil
}

// UserComplete.
func (m *pubMachine) completeCall(tc *zizek.TestCase) error {
	ch := m.js.PublishAsyncComplete()
	tc.Note(fmt.Sprintf("PublishAsyncComplete with %d pending", len(m.pending())))
	m.complete = append(m.complete, &completeCh{ch: ch})
	m.drained()
	return nil
}

// Disconnect + Reconnect: resetPendingAcksOnReconnect fails everything.
func (m *pubMachine) disconnect(tc *zizek.TestCase) error {
	tc.Assume(!m.closed)
	tc.Note(fmt.Sprintf("disconnect with %d pending", len(m.pending())))
	before := m.nc.Stats().Reconnects
	m.px.Cut()
	if !harness.Eventually(settle, func() bool { return m.nc.Stats().Reconnects > before && m.nc.IsConnected() }) {
		return errors.New("did not reconnect")
	}
	m.failAll(nats.ErrDisconnected)
	return m.afterRelease()
}

// --- extended (known-bug) rules ----------------------------------------------

// PubReg at the cap in a goroutine: a caller parked in the stall.
func (m *pubMachine) stallPublish(tc *zizek.TestCase) error {
	tc.Assume(!m.closed && m.stall == nil && len(m.pending()) >= machineMaxPA)
	s := &stalled{subj: m.subject(), started: time.Now(), done: make(chan struct{})}
	m.stall = s
	tc.Note("stalled publish " + s.subj)
	go func() {
		s.paf, s.err = m.js.PublishAsync(s.subj, []byte("x"),
			jetstream.WithRetryAttempts(machineRetries),
			jetstream.WithRetryWait(machineRetryWait),
			jetstream.WithStallWait(stalledStallWait))
		close(s.done)
	}()
	if !harness.Eventually(settle, func() bool { return countGoroutines("[select", "registerPAF") > 0 }) {
		return errors.New("stalled publish never parked")
	}
	return nil
}

// afterRelease: a slot may have been freed; a stalled caller must take it
// promptly (NoLostStallWakeup) and become a pending future.
func (m *pubMachine) afterRelease() error {
	m.drained()
	s := m.stall
	if s == nil || len(m.pending()) >= machineMaxPA {
		return nil
	}
	select {
	case <-s.done:
	case <-time.After(stalledWakeBudget):
		return fmt.Errorf("lost stall wakeup: %s still parked %v after pending dropped to %d (< max %d)",
			s.subj, time.Since(s.started).Round(time.Millisecond), len(m.pending()), machineMaxPA)
	}
	m.stall = nil
	if s.err != nil {
		return fmt.Errorf("stalled publish %s failed after a slot freed: %v", s.subj, s.err)
	}
	return m.addFuture(s.subj, s.paf)
}

// CleanupPublisher: every pending future fails with ErrJetStreamPublisherClosed.
func (m *pubMachine) cleanup(tc *zizek.TestCase) error {
	tc.Assume(!m.closed)
	tc.Note(fmt.Sprintf("CleanupPublisher with %d pending", len(m.pending())))
	m.js.CleanupPublisher()
	m.failAll(jetstream.ErrJetStreamPublisherClosed)
	return m.afterRelease()
}

// Close: nothing can be acked any more, so every pending future must fail
// with ErrConnectionClosed rather than hang (EventuallyResolved).
func (m *pubMachine) closeConn(tc *zizek.TestCase) error {
	tc.Assume(!m.closed && m.stall == nil)
	tc.Note(fmt.Sprintf("nc.Close with %d pending", len(m.pending())))
	m.nc.Close()
	m.closed = true
	for _, f := range m.all {
		if f.state == failed && f.wantErr == nil && f.seenErr == nil {
			f.wantErr = errAny
		}
	}
	m.failAll(nats.ErrConnectionClosed)
	m.drained()
	return nil
}

var errAny = errors.New("any error")

// --- invariants -------------------------------------------------------------

// MaxPendingRespected and pending agreement with the model.
func (m *pubMachine) pendingAgrees(tc *zizek.TestCase) error {
	want := len(m.pending())
	var got int
	if !harness.Eventually(settle, func() bool { got = m.js.PublishAsyncPending(); return got == want }) {
		return fmt.Errorf("PublishAsyncPending = %d, model has %d pending", got, want)
	}
	if got > machineMaxPA {
		return fmt.Errorf("pending %d exceeds max %d", got, machineMaxPA)
	}
	return nil
}

// ResolvedAtMostOnce / EventuallyResolved: each resolved future yields one
// value on exactly one of Ok()/Err(), with the model's outcome, and nothing
// after that; pending futures yield nothing.
func (m *pubMachine) futuresAgree(tc *zizek.TestCase) error {
	for _, f := range m.all {
		ok, errc := f.paf.Ok(), f.paf.Err()
		switch f.state {
		case pending:
			if len(ok)+len(errc) != 0 || f.seenAck != nil || f.seenErr != nil {
				return fmt.Errorf("%s resolved while the model has it pending", f.subj)
			}
		case acked, failed:
			if f.seenAck == nil && f.seenErr == nil {
				var timeout bool
				select {
				case a := <-ok:
					f.seenAck = a
				case e := <-errc:
					f.seenErr = e
				case <-time.After(settle):
					timeout = true
				}
				if timeout {
					return fmt.Errorf("%s never resolved (model: %v)", f.subj, f.state)
				}
			}
			if f.state == acked && f.seenAck == nil {
				return fmt.Errorf("%s: model acked, future failed with %v", f.subj, f.seenErr)
			}
			if f.state == failed {
				if f.seenErr == nil {
					return fmt.Errorf("%s: model failed (%v), future acked", f.subj, f.wantErr)
				}
				var apiErr *jetstream.APIError
				switch {
				case f.wantErr == errAny:
				case f.wantErr == nil && !errors.As(f.seenErr, &apiErr):
					return fmt.Errorf("%s: want an API error, got %v", f.subj, f.seenErr)
				case f.wantErr != nil && !errors.Is(f.seenErr, f.wantErr):
					return fmt.Errorf("%s: want %v, got %v", f.subj, f.wantErr, f.seenErr)
				}
			}
			if len(ok)+len(errc) != 0 {
				return fmt.Errorf("%s resolved twice", f.subj)
			}
		}
	}
	return nil
}

// ErrCbAtMostOnce + ack handler: one callback per resolution, of the right kind.
func (m *pubMachine) callbacksAgree(tc *zizek.TestCase) error {
	for _, f := range m.all {
		var e []error
		var a int
		check := func() bool {
			m.cbMu.Lock()
			defer m.cbMu.Unlock()
			e, a = m.errCb[f.subj], m.ackCb[f.subj]
			switch f.state {
			case pending:
				return len(e) == 0 && a == 0
			case acked:
				return len(e) == 0 && a == 1
			default:
				return len(e) == 1 && a == 0
			}
		}
		if !harness.Eventually(settle, check) {
			return fmt.Errorf("%s (model %v): %d error-handler calls %v, %d ack-handler calls", f.subj, f.state, len(e), e, a)
		}
	}
	return nil
}

// NoLostCompletion / no early completion: a PublishAsyncComplete channel is
// closed once the model's pending count has dropped to zero since it was
// taken, and not before.
func (m *pubMachine) completeAgrees(tc *zizek.TestCase) error {
	m.drained()
	var open []*completeCh
	for i, c := range m.complete {
		if c.mustClose {
			if !closedWithin(c.ch, settle) {
				return fmt.Errorf("PublishAsyncComplete #%d still open although pending reached 0", i)
			}
			continue
		}
		select {
		case <-c.ch:
			return fmt.Errorf("PublishAsyncComplete #%d closed with %d pending", i, len(m.pending()))
		default:
		}
		open = append(open, c)
	}
	m.complete = open
	return nil
}

// --- driver -------------------------------------------------------------------

// ruleStats counts the rule applications that ran to completion (not
// rejected by an assumption), so a run can show what it exercised.
type ruleStats struct {
	mu sync.Mutex
	n  map[string]int
}

func counted(s *ruleStats, r zizek.Rule[pubMachine]) zizek.Rule[pubMachine] {
	apply := r.Apply
	r.Apply = func(m *pubMachine, tc *zizek.TestCase) error {
		err := apply(m, tc) // an Assume rejection panics past the count
		s.mu.Lock()
		s.n[r.Name]++
		s.mu.Unlock()
		return err
	}
	return r
}

func runMachine(t *testing.T, extra []zizek.Rule[pubMachine], cfg zizek.Config) error {
	stats := &ruleStats{n: map[string]int{}}
	defer func() { t.Logf("rule applications: %v", stats.n) }()
	srv := harness.RunServer(t, true)
	px := harness.NewProxy(t, srv.Addr())
	var runs atomic.Int64

	property := func(tc *zizek.TestCase) error {
		run := runs.Add(1)
		m := &pubMachine{tc: tc, px: px, run: run,
			futs: zizek.NewPool[*fut](tc), errCb: map[string][]error{}, ackCb: map[string]int{}}
		side, err := nats.Connect(srv.URL())
		if err != nil {
			return err
		}
		defer side.Close()
		m.resp = &responder{reqs: map[string][]string{}, side: side}
		if _, err := side.Subscribe(fmt.Sprintf("ctl.%d.>", run), m.resp.handle); err != nil {
			return err
		}
		if err := side.Flush(); err != nil {
			return err
		}
		nc, err := nats.Connect(px.URL(), nats.MaxReconnects(-1),
			nats.ReconnectWait(10*time.Millisecond), nats.ReconnectJitter(0, 0))
		if err != nil {
			return err
		}
		defer nc.Close()
		m.nc = nc
		m.js, err = jetstream.New(nc,
			jetstream.WithPublishAsyncMaxPending(machineMaxPA),
			jetstream.WithPublishAsyncErrHandler(func(_ jetstream.JetStream, msg *nats.Msg, err error) {
				m.cbMu.Lock()
				m.errCb[msg.Subject] = append(m.errCb[msg.Subject], err)
				m.cbMu.Unlock()
			}),
			jetstream.WithPublishAsyncAckHandler(func(_ jetstream.JetStream, msg *nats.Msg, _ *jetstream.PubAck) {
				m.cbMu.Lock()
				m.ackCb[msg.Subject]++
				m.cbMu.Unlock()
			}))
		if err != nil {
			return err
		}
		rules := []zizek.Rule[pubMachine]{
			{Name: "publish", Apply: (*pubMachine).publish},
			{Name: "ack", Apply: (*pubMachine).ack},
			{Name: "error_ack", Apply: (*pubMachine).errAck},
			{Name: "no_responders", Apply: (*pubMachine).noResponders},
			{Name: "late_reply", Apply: (*pubMachine).duplicate},
			{Name: "observe", Apply: (*pubMachine).observe},
			{Name: "complete", Apply: (*pubMachine).completeCall},
			{Name: "disconnect", Apply: (*pubMachine).disconnect},
			{Name: "cleanup", Apply: (*pubMachine).cleanup},
		}
		rules = append(rules, extra...)
		for i := range rules {
			rules[i] = counted(stats, rules[i])
		}
		machine := zizek.Machine[pubMachine]{
			Rules: rules,
			Invariants: []zizek.Rule[pubMachine]{
				{Name: "pending_agrees", Apply: (*pubMachine).pendingAgrees},
				{Name: "futures_agree", Apply: (*pubMachine).futuresAgree},
				{Name: "callbacks_agree", Apply: (*pubMachine).callbacksAgree},
				{Name: "complete_agrees", Apply: (*pubMachine).completeAgrees},
			},
		}
		return zizek.RunSteps(m, machine, tc, 25)
	}
	return zizek.Check(property, cfg)
}

// TestPublishAsyncMachine: acks, API-error acks, 503 retries (exhausted or
// not), late/duplicate replies, early Ok()/Err(), PublishAsyncComplete, the
// max-pending stall error, reconnects and CleanupPublisher -- the paths the model finds
// correct in Baseline.cfg -- must keep every invariant.
func TestPublishAsyncMachine(t *testing.T) {
	if err := runMachine(t, nil, zizek.Config{Seed: 7, Examples: 80, SuppressTooSlow: true}); err != nil {
		t.Fatal(err)
	}
}

// TestPublishAsyncMachineStallCleanup adds a caller parked in the
// max-pending stall, so that CleanupPublisher can meet it (PUB-2, and the
// PUB-6 window right after the wakeup). Before the fixes zizek shrank to
// publish, publish, error_ack, publish, stalled_publish, cleanup -> lost
// stall wakeup.
func TestPublishAsyncMachineStallCleanup(t *testing.T) {
	extra := []zizek.Rule[pubMachine]{{Name: "stalled_publish", Apply: (*pubMachine).stallPublish}}
	if err := runMachine(t, extra, zizek.Config{Seed: 7, Examples: 80, SuppressTooSlow: true}); err != nil {
		t.Fatal(err)
	}
}

// TestPublishAsyncMachineClose adds nc.Close (PUB-5). Before the fix zizek
// shrank to publish, close -> a future that never resolves.
func TestPublishAsyncMachineClose(t *testing.T) {
	extra := []zizek.Rule[pubMachine]{{Name: "close", Apply: (*pubMachine).closeConn}}
	if err := runMachine(t, extra, zizek.Config{Seed: 7, Examples: 80, SuppressTooSlow: true}); err != nil {
		t.Fatal(err)
	}
}
