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

package conn

// Stateful tests derived from models/ConnLifecycle. Each machine's rules are
// the model's actions, performed on the real client (through a TCP proxy and
// a fault-injecting dialer standing in for the model's environment), and its
// invariants are the model's invariants, checked on settled state after every
// step.
//
// Every machine runs twice, and both must pass:
//   - TestXxxMachine runs every rule and checks the cheap invariants after
//     every step.
//   - TestXxxMachineFocused has one subtest per invariant that a *_bug.cfg
//     config of the model violates (CONN-1, 3, 5, 6, 7, 9, 10 before they
//     were fixed): more examples, with the rules that lead to that bug and
//     the settling that invariant needs. Each of them found its bug with a
//     shrunk counterexample on the unfixed code (see the model README).

import (
	"bytes"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/modeltest/harness"
	zizek "github.com/wallyqs/zizek.go"
)

func machineConfig(strict bool) zizek.Config {
	cfg := zizek.Config{Examples: 12, Seed: 7, SuppressTooSlow: true, SuppressFilterTooMuch: true}
	if strict {
		cfg.Examples = 60
		cfg.MaxShrinks = 10
		cfg.MaxStall = 8
	}
	return cfg
}

// ---------------------------------------------------------------------------
// Lifecycle machine (ConnLifecycle.tla)
// ---------------------------------------------------------------------------

type flushRec struct {
	id      int
	start   time.Time
	done    chan struct{}
	err     error
	checked bool
	split   bool  // the status changed between Publish and Flush
	changes int64 // status changes seen before the Publish
}

type lcMachine struct {
	t      testing.TB
	strict bool
	focus  string // strict: the one focused invariant enforced

	srv *harness.Server
	p   *harness.Proxy
	d   *faultDialer
	nc  *nats.Conn
	sc  *nats.Conn         // subscriber, direct to the server
	rx  *nats.Subscription // on sc
	got map[int]bool       // message ids rx received

	log     cbLog
	flushes []*flushRec
	next    int

	blocked, refusing, closed bool
	hold                      *readHold
	faults                    []*writeFault

	lazy        chan nats.Status // StatusChanged listener read only at checks
	lastLazy    nats.Status      // last status it delivered (-1: none)
	changes     atomic.Int64     // status changes seen by a prompt reader
	seenChanges int64
	stopPrompt  chan struct{}
	promptDone  sync.WaitGroup
}

const lcSubject = "lc.m"

func newLC(t testing.TB, focus string) (*lcMachine, error) {
	m := &lcMachine{t: t, strict: focus != "", focus: focus, got: map[int]bool{}, stopPrompt: make(chan struct{}), lastLazy: -1}
	m.srv = harness.RunServer(t, false)
	m.p = harness.NewProxy(t, m.srv.Addr())
	sc, err := nats.Connect(m.srv.URL(), nats.ErrorHandler(quietErrs))
	if err != nil {
		return nil, err
	}
	m.sc = sc
	if m.rx, err = sc.SubscribeSync(lcSubject); err != nil {
		return nil, err
	}
	sc.Flush()
	m.d = &faultDialer{}
	opts := append(m.log.opts(),
		nats.ErrorHandler(quietErrs),
		nats.SetCustomDialer(m.d),
		nats.ReconnectWait(25*time.Millisecond),
		nats.ReconnectJitter(0, 0),
		nats.MaxReconnects(200),
		nats.Timeout(500*time.Millisecond),
	)
	if m.nc, err = nats.Connect(m.p.URL(), opts...); err != nil {
		return nil, err
	}
	m.lazy = m.nc.StatusChanged(nats.CONNECTED, nats.RECONNECTING, nats.CLOSED)
	prompt := m.nc.StatusChanged()
	m.promptDone.Add(1)
	go func() {
		defer m.promptDone.Done()
		for {
			select {
			case <-prompt:
				m.changes.Add(1)
			case <-m.stopPrompt:
				return
			}
		}
	}()
	return m, nil
}

func (m *lcMachine) teardown() {
	if m.hold != nil {
		close(m.hold.release)
		m.hold = nil
	}
	if m.nc != nil {
		m.nc.Close()
	}
	close(m.stopPrompt)
	m.promptDone.Wait()
	if m.sc != nil {
		m.sc.Close()
	}
	m.p.Close()
	m.srv.Shutdown()
}

// rules ----------------------------------------------------------------------

// FlushStart(f): Publish a message, then FlushTimeout in a goroutine.
//
// Flush only vouches for messages that travel in the same stream as its
// PING: a message the flusher wrote to a connection that then died, followed
// by a Flush issued after the disconnect and answered on the next
// connection, is lost by design (at-most-once). Such pairs are marked and not
// checked by flushIntegrity: the status must not change between the Publish
// and the Flush call.
//
// The step returns only once the Flush has queued its PING (it was written,
// or the pending buffer grew), as it would in a goroutine that calls Publish
// and then Flush: a Flush goroutine that is scheduled late, after later
// steps, would otherwise queue its PING behind a reconnect that the split
// check above did not see.
func (m *lcMachine) publishFlush(tc *zizek.TestCase) error {
	id := m.next
	m.next++
	st0, ch0 := m.nc.Status(), m.changes.Load()
	perr := m.nc.Publish(lcSubject, []byte(strconv.Itoa(id)))
	r := &flushRec{id: id, start: time.Now(), done: make(chan struct{}), changes: ch0}
	pings0 := m.d.pings()
	buffered0, _ := m.nc.Buffered()
	go func() {
		if m.nc.Status() != st0 || m.changes.Load() != ch0 {
			r.split = true
		}
		r.err = m.nc.FlushTimeout(time.Second)
		close(r.done)
	}()
	harness.Eventually(250*time.Millisecond, func() bool {
		select {
		case <-r.done:
			return true
		default:
		}
		if m.d.pings() > pings0 {
			return true
		}
		b, err := m.nc.Buffered()
		return err != nil || (st0 == nats.RECONNECTING && m.nc.Status() == nats.RECONNECTING && b > buffered0)
	})
	m.flushes = append(m.flushes, r)
	tc.Note(fmt.Sprintf("publish %d (err %v) + flush, status %v", id, perr, st0))
	return nil
}

// ServerDrop: the network drops the client's socket. When the client can
// reconnect, wait until it has noticed, so that the step's effects have
// settled before the invariants run.
func (m *lcMachine) cut(tc *zizek.TestCase) error {
	before := m.nc.Stats().Reconnects
	connected := m.nc.IsConnected()
	m.p.Cut()
	if connected && m.networkOK() {
		harness.Eventually(3*time.Second, func() bool { return m.nc.Stats().Reconnects > before || m.nc.IsClosed() })
	}
	tc.Note("cut")
	return nil
}

// settleDisconnected waits until the client has noticed a cut (unless a held
// read keeps its readLoop from noticing).
func (m *lcMachine) settleDisconnected() {
	if m.hold == nil && !m.closed {
		harness.Eventually(2*time.Second, func() bool { return !m.nc.IsConnected() })
	}
}

// DRHsFail: the proxy accepts and closes, so reconnect handshakes fail.
func (m *lcMachine) block(tc *zizek.TestCase) error {
	tc.Assume(!m.blocked)
	m.blocked = true
	dials := m.d.dials.Load()
	m.p.Block()
	m.settleDisconnected()
	// Let one reconnect attempt dial (and fail its handshake).
	harness.Eventually(time.Second, func() bool { return m.d.dials.Load() > dials || m.nc.IsClosed() })
	time.Sleep(5 * time.Millisecond)
	tc.Note("block")
	return nil
}

func (m *lcMachine) unblock(tc *zizek.TestCase) error {
	tc.Assume(m.blocked)
	m.blocked = false
	m.p.Unblock()
	tc.Note("unblock")
	return nil
}

// DRDialFail: dials fail (server unreachable).
func (m *lcMachine) refuse(tc *zizek.TestCase) error {
	tc.Assume(!m.refusing)
	m.refusing = true
	m.d.refuse.Store(true)
	m.p.Cut()
	m.settleDisconnected()
	tc.Note("refuse dials + cut")
	return nil
}

func (m *lcMachine) allow(tc *zizek.TestCase) error {
	tc.Assume(m.refusing)
	m.refusing = false
	m.d.refuse.Store(false)
	tc.Note("allow dials")
	return nil
}

func (m *lcMachine) forceReconnect(tc *zizek.TestCase) error {
	err := m.nc.ForceReconnect()
	tc.Note(fmt.Sprintf("ForceReconnect -> %v", err))
	return nil
}

func (m *lcMachine) userClose(tc *zizek.TestCase) error {
	tc.Assume(!m.closed)
	m.closed = true
	m.nc.Close()
	tc.Note("Close")
	return nil
}

// AllowWriteFail: the next write carrying a PUB fails half way.
func (m *lcMachine) armWriteFault(tc *zizek.TestCase) error {
	closeConn, err := tc.Boolean()
	if err != nil {
		return err
	}
	m.faults = append(m.faults, m.d.armWrite("PUB "+lcSubject, closeConn))
	tc.Note(fmt.Sprintf("arm partial write (close socket: %v)", closeConn))
	return nil
}

// RLRead / RLParsePong split: the readLoop has a PONG in hand and is
// preempted before parsing it.
func (m *lcMachine) holdPong(tc *zizek.TestCase) error {
	tc.Assume(m.hold == nil && !m.closed && m.nc.Status() == nats.CONNECTED)
	m.hold = m.d.armRead("PONG")
	tc.Note("hold next PONG read on the current socket")
	return nil
}

func (m *lcMachine) releasePong(tc *zizek.TestCase) error {
	tc.Assume(m.hold != nil)
	close(m.hold.release)
	m.hold = nil
	tc.Note("release held read")
	return nil
}

// invariants -----------------------------------------------------------------

func (m *lcMachine) networkOK() bool { return !m.blocked && !m.refusing && m.hold == nil }

// Callbacks: ClosedCB at most once, nothing after it.
func (m *lcMachine) callbacks(_ *zizek.TestCase) error {
	evs := m.log.get()
	if n := m.log.count("closed"); n > 1 {
		return fmt.Errorf("ClosedCB ran %d times: %v", n, evs)
	}
	if i := slices.Index(evs, "closed"); i >= 0 && i != len(evs)-1 {
		return fmt.Errorf("callback after ClosedCB: %v", evs)
	}
	return nil
}

// NoDoubleDisconnect (CONN-9).
func (m *lcMachine) noDoubleDisconnect(_ *zizek.TestCase) error {
	evs := m.log.get()
	run := 0
	for _, e := range evs {
		switch e {
		case "disconnected":
			run++
			if run > 1 {
				return fmt.Errorf("two DisconnectedErrCB without a ReconnectedCB between them: %v", evs)
			}
		case "reconnected":
			run = 0
		}
	}
	return nil
}

// Every Flush returns by its timeout (plus slack).
func (m *lcMachine) flushReturns(_ *zizek.TestCase) error {
	for _, r := range m.flushes {
		select {
		case <-r.done:
		default:
			if time.Since(r.start) > 4*time.Second {
				return fmt.Errorf("FlushTimeout(1s) for message %d has not returned after %v", r.id, time.Since(r.start))
			}
		}
	}
	return nil
}

// FlushIntegrity (CONN-5, CONN-6): Flush returned nil => the server got the
// message published before it, so the subscriber receives it.
func (m *lcMachine) flushIntegrity(tc *zizek.TestCase) error {
	if m.networkOK() {
		// Settle: let outstanding flushes complete (they time out after 1s).
		for _, r := range m.flushes {
			select {
			case <-r.done:
			case <-time.After(1500 * time.Millisecond):
			}
		}
	}
	for _, r := range m.flushes {
		if r.checked {
			continue
		}
		select {
		case <-r.done:
		default:
			continue
		}
		r.checked = true
		if r.err != nil || r.split {
			continue
		}
		ok := harness.Eventually(2*time.Second, func() bool {
			for {
				msg, err := m.rx.NextMsg(time.Millisecond)
				if err != nil {
					break
				}
				id, _ := strconv.Atoi(string(msg.Data))
				m.got[id] = true
			}
			return m.got[r.id]
		})
		if !ok {
			if m.lostBeforePing(r.id) {
				tc.Note(fmt.Sprintf("message %d was lost in a failed write without the Flush's PING: not checked", r.id))
				continue
			}
			if m.pingElsewhere(r) {
				tc.Note(fmt.Sprintf("message %d was lost with a connection that its Flush's PING did not travel on: not checked", r.id))
				continue
			}
			return fmt.Errorf("Flush after message %d returned nil, but the message never reached the server", r.id)
		}
	}
	return nil
}

// lostBeforePing reports whether message id was in a write that failed (and
// killed its socket) with no PING behind it: the flusher wrote the PUB on its
// own, the Flush's PING went out later, on the next connection. That loss is
// the at-most-once delivery of core NATS, not a broken Flush.
func (m *lcMachine) lostBeforePing(id int) bool {
	pub := []byte(fmt.Sprintf("PUB %s %d\r\n%d\r\n", lcSubject, len(strconv.Itoa(id)), id))
	for _, f := range m.faults {
		select {
		case <-f.fired:
		default:
			continue
		}
		if i := bytes.Index(f.data, pub); i >= 0 && !bytes.Contains(f.data[i:], []byte("PING")) {
			return true
		}
	}
	return false
}

// pingElsewhere reports whether the status changed after the message of r
// was published and no PING followed the message on the socket it was
// written to (if it was written at all): the message was lost with that
// connection, written to a dead socket or still in the write buffer that the
// reconnect drops, while the Flush's PING went on the next connection. That
// is core NATS at-most-once delivery, not a broken Flush. The "split" check
// in publishFlush misses it when the disconnect lands between its status
// check and FlushTimeout taking the connection lock.
func (m *lcMachine) pingElsewhere(r *flushRec) bool {
	pub := []byte(fmt.Sprintf("PUB %s %d\r\n%d\r\n", lcSubject, len(strconv.Itoa(r.id)), r.id))
	return m.changes.Load() > r.changes && !m.d.pingFollows(pub)
}

// Liveness, checked on settled state: with the network healthy, an open
// connection is (re)connected within a few back-off periods (CONN-5), and it
// is never closed by running out of attempts (CONN-7: only Close, or
// MaxReconnects(200) at 25ms -- 5s of failed attempts -- may close it). It
// also settles the state for the other invariants; when another known-bug
// invariant is the focus it only waits.
func (m *lcMachine) reconnects(tc *zizek.TestCase) error {
	if m.closed || !m.networkOK() {
		return nil
	}
	enforce := !m.strict || m.focus == "reconnects"
	if m.nc.IsClosed() {
		if enforce {
			return fmt.Errorf("connection closed without Close: reconnect attempts exhausted")
		}
		m.closed = true
		return nil
	}
	if !harness.Eventually(2*time.Second, func() bool { return m.nc.IsConnected() }) && enforce {
		return fmt.Errorf("network healthy but the client is still %v after 2s (ReconnectWait 25ms)", m.nc.Status())
	}
	return nil
}

// CONN-1: a listener that reads only now and then still learns the status
// the connection settled in: the last event it has received equals the
// current status, once the status has changed since the previous check.
func (m *lcMachine) lazyListener(tc *zizek.TestCase) error {
	st1 := m.nc.Status()
	var evs []nats.Status
	for drained := false; !drained; {
		select {
		case s := <-m.lazy:
			evs = append(evs, s)
			m.lastLazy = s
		default:
			drained = true
		}
	}
	// The prompt reader may lag; give it a moment so that "changed" is
	// counted no later than the listener saw it.
	time.Sleep(5 * time.Millisecond)
	changes := m.changes.Load()
	st2 := m.nc.Status()
	since := changes - m.seenChanges
	m.seenChanges = changes
	if st1 != st2 || since == 0 || (st1 != nats.CONNECTED && st1 != nats.CLOSED) {
		return nil
	}
	if m.lastLazy != st1 {
		return fmt.Errorf("status changed %d time(s) and is now %v, but the StatusChanged listener received %v this time and its last event is %v",
			since, st1, evs, statusName(m.lastLazy))
	}
	return nil
}

// logFailure logs a failing example as it happened (before shrinking) when
// MODELTEST_LOG_FAILURES=1: with real concurrency a failing example may not
// fail again on replay, and zizek's *zizek.Flaky then carries the original
// error and blob but not the notes (the rule trace) logged here.
func logFailure(t testing.TB, tc *zizek.TestCase, err error) {
	if os.Getenv("MODELTEST_LOG_FAILURES") != "1" {
		return
	}
	t.Logf("failing example: %v\n  %s", err, strings.Join(tc.Notes(), "\n  "))
}

func statusName(s nats.Status) string {
	if s < 0 {
		return "none"
	}
	return s.String()
}

// lcProperty runs the lifecycle machine. With focus == "" it uses every rule
// and the invariants that need no extra settling; otherwise it uses the
// rules that reach the bug behind the named invariant and enforces it.
func lcProperty(t testing.TB, focus string) func(tc *zizek.TestCase) error {
	return func(tc *zizek.TestCase) error {
		m, err := newLC(t, focus)
		if err != nil {
			return err
		}
		defer m.teardown()
		all := map[string]func(*lcMachine, *zizek.TestCase) error{
			"publishFlush":   (*lcMachine).publishFlush,
			"cut":            (*lcMachine).cut,
			"block":          (*lcMachine).block,
			"unblock":        (*lcMachine).unblock,
			"refuse":         (*lcMachine).refuse,
			"allow":          (*lcMachine).allow,
			"close":          (*lcMachine).userClose,
			"forceReconnect": (*lcMachine).forceReconnect,
			"armWriteFault":  (*lcMachine).armWriteFault,
			"holdPong":       (*lcMachine).holdPong,
			"releasePong":    (*lcMachine).releasePong,
		}
		// The rules each run uses: all of them, or, for a focused
		// invariant, the model actions that lead to its bug.
		names := map[string][]string{
			"":                   {"publishFlush", "cut", "block", "unblock", "refuse", "allow", "close", "forceReconnect", "armWriteFault", "holdPong", "releasePong"},
			"lazyListener":       {"publishFlush", "cut", "block", "unblock", "close"},
			"flushIntegrity":     {"publishFlush", "cut", "refuse", "allow", "forceReconnect", "armWriteFault", "holdPong", "releasePong"},
			"reconnects":         {"publishFlush", "cut", "block", "unblock", "refuse", "allow", "forceReconnect", "armWriteFault"},
			"noDoubleDisconnect": {"cut", "block", "unblock", "refuse", "allow", "forceReconnect", "close"},
		}[focus]
		var rules []zizek.Rule[lcMachine]
		for _, n := range names {
			rules = append(rules, zizek.Rule[lcMachine]{Name: n, Apply: all[n]})
		}
		inv := []zizek.Rule[lcMachine]{
			{Name: "callbacks", Apply: (*lcMachine).callbacks},
			{Name: "flushReturns", Apply: (*lcMachine).flushReturns},
			{Name: "reconnects", Apply: (*lcMachine).reconnects},
		}
		switch focus {
		case "":
			inv = append(inv,
				zizek.Rule[lcMachine]{Name: "noDoubleDisconnect", Apply: (*lcMachine).noDoubleDisconnect},
				zizek.Rule[lcMachine]{Name: "lazyListener", Apply: (*lcMachine).lazyListener})
		case "flushIntegrity":
			inv = append(inv, zizek.Rule[lcMachine]{Name: focus, Apply: (*lcMachine).flushIntegrity})
		case "noDoubleDisconnect":
			inv = append(inv, zizek.Rule[lcMachine]{Name: focus, Apply: (*lcMachine).noDoubleDisconnect})
		case "lazyListener":
			inv = append(inv, zizek.Rule[lcMachine]{Name: focus, Apply: (*lcMachine).lazyListener})
		}
		if err := zizek.RunSteps(m, zizek.Machine[lcMachine]{Rules: rules, Invariants: inv}, tc, 12); err != nil {
			logFailure(t, tc, err)
			return err
		}
		// Final: every flush returns.
		for _, r := range m.flushes {
			select {
			case <-r.done:
			case <-time.After(4 * time.Second):
				return fmt.Errorf("FlushTimeout(1s) for message %d never returned", r.id)
			}
		}
		return nil
	}
}

func TestLifecycleMachine(t *testing.T) {
	if err := zizek.Check(lcProperty(t, ""), machineConfig(false)); err != nil {
		t.Fatal(err)
	}
}

// One subtest per invariant, with the findings it caught before they were
// fixed: lazyListener (CONN-1), flushIntegrity (CONN-5, CONN-6), reconnects
// (CONN-5, CONN-7), noDoubleDisconnect (CONN-9).
func TestLifecycleMachineFocused(t *testing.T) {
	for _, focus := range []string{"lazyListener", "flushIntegrity", "reconnects", "noDoubleDisconnect"} {
		t.Run(focus, func(t *testing.T) {
			if err := zizek.Check(lcProperty(t, focus), machineConfig(true)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Subscription machine (SubResend.tla)
// ---------------------------------------------------------------------------

type srMachine struct {
	t testing.TB

	srv *harness.Server
	p   *harness.Proxy
	nc  *nats.Conn
	pub *nats.Conn
	sub *nats.Subscription

	published  int
	userGot    int
	lastSeq    int
	autoMax    int
	gotAtAuto  int
	removed    bool // Unsubscribe called, or AutoUnsubscribe/Drain completed
	drained    bool
	unsubbed   bool
	outage     bool
	handlerRun atomic.Int64
}

const srSubject = "sr.m"

func newSR(t testing.TB, queue bool) (*srMachine, error) {
	m := &srMachine{t: t, gotAtAuto: -1, lastSeq: -1}
	m.srv = harness.RunServer(t, false)
	m.p = harness.NewProxy(t, m.srv.Addr())
	var err error
	if m.pub, err = nats.Connect(m.srv.URL(), nats.ErrorHandler(quietErrs)); err != nil {
		return nil, err
	}
	if m.nc, err = nats.Connect(m.p.URL(), nats.ErrorHandler(quietErrs),
		nats.ReconnectWait(20*time.Millisecond), nats.MaxReconnects(-1), nats.ReconnectJitter(0, 0)); err != nil {
		return nil, err
	}
	if queue {
		m.sub, err = m.nc.QueueSubscribeSync(srSubject, "q")
	} else {
		m.sub, err = m.nc.SubscribeSync(srSubject)
	}
	if err != nil {
		return nil, err
	}
	m.sub.SetClosedHandler(func(string) { m.handlerRun.Add(1) })
	m.nc.Flush()
	return m, nil
}

func (m *srMachine) teardown() {
	m.nc.Close()
	m.pub.Close()
	m.p.Close()
	m.srv.Shutdown()
}

func (m *srMachine) publish(tc *zizek.TestCase) error {
	m.pub.Publish(srSubject, []byte(strconv.Itoa(m.published)))
	m.pub.Flush()
	tc.Note(fmt.Sprintf("publish %d (client %v)", m.published, m.nc.Status()))
	m.published++
	return nil
}

// SyncNext.
func (m *srMachine) next(tc *zizek.TestCase) error {
	msg, err := m.sub.NextMsg(20 * time.Millisecond)
	if err != nil {
		tc.Note(fmt.Sprintf("NextMsg -> %v", err))
		return nil
	}
	seq, _ := strconv.Atoi(string(msg.Data))
	if seq <= m.lastSeq {
		return fmt.Errorf("message %d delivered after %d", seq, m.lastSeq)
	}
	m.lastSeq = seq
	m.userGot++
	tc.Note(fmt.Sprintf("NextMsg -> %d (total %d)", seq, m.userGot))
	return nil
}

func (m *srMachine) autoUnsubscribe(tc *zizek.TestCase) error {
	tc.Assume(m.gotAtAuto < 0)
	n, err := tc.Integer(1, 4, 1)
	if err != nil {
		return err
	}
	m.gotAtAuto = m.userGot
	m.autoMax = int(n)
	err = m.sub.AutoUnsubscribe(int(n))
	tc.Note(fmt.Sprintf("AutoUnsubscribe(%d) after %d delivered -> %v (client %v)", n, m.userGot, err, m.nc.Status()))
	return nil
}

func (m *srMachine) unsubscribe(tc *zizek.TestCase) error {
	tc.Assume(!m.unsubbed)
	m.unsubbed = true
	err := m.sub.Unsubscribe()
	tc.Note(fmt.Sprintf("Unsubscribe -> %v (client %v)", err, m.nc.Status()))
	return nil
}

func (m *srMachine) drain(tc *zizek.TestCase) error {
	tc.Assume(!m.drained)
	m.drained = true
	err := m.sub.Drain()
	tc.Note(fmt.Sprintf("Drain -> %v (client %v)", err, m.nc.Status()))
	return nil
}

// Disconnect + Reconnect in one step.
func (m *srMachine) cut(tc *zizek.TestCase) error {
	tc.Assume(!m.outage)
	m.p.Cut()
	harness.Eventually(3*time.Second, func() bool { return m.nc.IsConnected() })
	tc.Note("cut, reconnected")
	return nil
}

// Disconnect; later rules run while RECONNECTING until restore.
func (m *srMachine) outageStart(tc *zizek.TestCase) error {
	tc.Assume(!m.outage)
	m.outage = true
	m.p.Block()
	harness.Eventually(3*time.Second, func() bool { return m.nc.IsReconnecting() })
	tc.Note("outage")
	return nil
}

func (m *srMachine) outageEnd(tc *zizek.TestCase) error {
	tc.Assume(m.outage)
	m.outage = false
	m.p.Unblock()
	harness.Eventually(3*time.Second, func() bool { return m.nc.IsConnected() })
	tc.Note("restored")
	return nil
}

// MaxRespected.
func (m *srMachine) maxRespected(_ *zizek.TestCase) error {
	if m.gotAtAuto < 0 {
		return nil
	}
	limit := max(m.gotAtAuto, m.autoMax)
	if m.userGot > limit {
		return fmt.Errorf("AutoUnsubscribe(%d) (called after %d messages) but %d delivered", m.autoMax, m.gotAtAuto, m.userGot)
	}
	return nil
}

// ClosedHandlerOnce (CONN-10).
func (m *srMachine) closedHandlerOnce(_ *zizek.TestCase) error {
	if m.drained && m.sub.IsValid() {
		// Settle: give checkDrained (Flush, then a 100ms poll) time to act.
		time.Sleep(150 * time.Millisecond)
	}
	if n := m.handlerRun.Load(); n > 1 {
		return fmt.Errorf("closed handler ran %d times", n)
	}
	return nil
}

// NoLeakedInterest (CONN-3, CONN-4): settled, a subscription the client has
// removed has no interest left on the server.
func (m *srMachine) noLeakedInterest(_ *zizek.TestCase) error {
	if m.outage || !m.nc.IsConnected() {
		return nil
	}
	// Let a Drain finish (its checkDrained polls every 100ms).
	if m.drained {
		harness.Eventually(time.Second, func() bool { return !m.sub.IsValid() })
	}
	if m.sub.IsValid() {
		return nil
	}
	if err := m.nc.FlushTimeout(2 * time.Second); err != nil {
		return nil
	}
	if !harness.Eventually(200*time.Millisecond, func() bool { return serverSubs(m.t, m.srv, srSubject) == 0 }) {
		return fmt.Errorf("client removed the subscription (delivered %d), but the server still holds %d subscription(s) on it",
			m.userGot, serverSubs(m.t, m.srv, srSubject))
	}
	return nil
}

// srProperty runs the subscription machine; focus as for lcProperty.
func srProperty(t testing.TB, focus string) func(tc *zizek.TestCase) error {
	return func(tc *zizek.TestCase) error {
		queue, err := tc.Boolean()
		if err != nil {
			return err
		}
		m, err := newSR(t, queue)
		if err != nil {
			return err
		}
		defer m.teardown()
		tc.Note(fmt.Sprintf("queue subscription: %v", queue))
		rules := []zizek.Rule[srMachine]{
			{Name: "publish", Apply: (*srMachine).publish},
			{Name: "next", Apply: (*srMachine).next},
			{Name: "autoUnsubscribe", Apply: (*srMachine).autoUnsubscribe},
			{Name: "unsubscribe", Apply: (*srMachine).unsubscribe},
			{Name: "cut", Apply: (*srMachine).cut},
			{Name: "outageStart", Apply: (*srMachine).outageStart},
			{Name: "outageEnd", Apply: (*srMachine).outageEnd},
		}
		rules = append(rules, zizek.Rule[srMachine]{Name: "drain", Apply: (*srMachine).drain})
		inv := []zizek.Rule[srMachine]{
			{Name: "maxRespected", Apply: (*srMachine).maxRespected},
		}
		switch focus {
		case "":
			inv = append(inv,
				zizek.Rule[srMachine]{Name: "noLeakedInterest", Apply: (*srMachine).noLeakedInterest},
				zizek.Rule[srMachine]{Name: "closedHandlerOnce", Apply: (*srMachine).closedHandlerOnce})
		case "noLeakedInterest":
			inv = append(inv, zizek.Rule[srMachine]{Name: focus, Apply: (*srMachine).noLeakedInterest})
		case "closedHandlerOnce":
			inv = append(inv, zizek.Rule[srMachine]{Name: focus, Apply: (*srMachine).closedHandlerOnce})
		}
		err = zizek.RunSteps(m, zizek.Machine[srMachine]{Rules: rules, Invariants: inv}, tc, 16)
		if err != nil {
			logFailure(t, tc, err)
		}
		return err
	}
}

func TestSubscriptionMachine(t *testing.T) {
	if err := zizek.Check(srProperty(t, ""), machineConfig(false)); err != nil {
		t.Fatal(err)
	}
}

// One subtest per invariant, with the findings it caught before they were
// fixed: noLeakedInterest (CONN-3, CONN-4), closedHandlerOnce (CONN-10).
func TestSubscriptionMachineFocused(t *testing.T) {
	for _, focus := range []string{"noLeakedInterest", "closedHandlerOnce"} {
		t.Run(focus, func(t *testing.T) {
			if err := zizek.Check(srProperty(t, focus), machineConfig(true)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
