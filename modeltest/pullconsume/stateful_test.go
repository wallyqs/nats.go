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

package pullconsume

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/modeltest/harness"
	"github.com/wallyqs/zizek.go"
)

// Stateful tests derived from models/PullConsume. The rules are the
// environment actions of the TLA+ models played against a real server
// through the NATSProxy:
//
//	publish         Env "server delivers" (the stream gets messages)
//	cut             Env disconnect + reconnect (the server keeps waiting pulls)
//	lateHeartbeats  LateHeartbeats: idle heartbeats are lost for 2x heartbeat,
//	                so the heartbeat timer fires (Timer process)
//	wait            time passes: pulls expire with a 408 (Env expire)
//	settle          liveness probe for NoSilentStall
//	next            (Messages) the Nexter process
//
// and the invariants are the models' invariants on what the library does
// on the wire and to the user: StopAfterRespected, BatchWithinMax,
// NoInvalidPull, NoSlowConsumerDrop, NoSilentStall (a message published on
// a healthy connection is delivered promptly with no ErrNoHeartbeat), and
// on teardown NoDeadlock (every consume goroutine returns, Closed fires)
// and DrainKeepsBuffered.
//
// By default the draws stay clear of the known bugs (no StopAfter, no
// reconnect with an iterator backlog, ...), so the test guards the rest of
// the state space. With MODELTEST_KNOWN_BUGS=1 the whole space is explored
// and the run is expected to fail on one of the README findings.

const (
	stExpiry    = time.Second
	stHeartbeat = 500 * time.Millisecond
)

func knownBugsMode() bool { return os.Getenv("MODELTEST_KNOWN_BUGS") == "1" }

// logFirst wraps a property so that the first failure is logged with its
// notes as soon as it happens. These properties run against a real server
// and are not perfectly replayable: when a shrunk replay stops failing,
// zizek reports "shrunk example no longer fails" without the original
// cause, so the cause is kept here.
func logFirst(t testing.TB, property func(*zizek.TestCase) error) func(*zizek.TestCase) error {
	var once sync.Once
	return func(tc *zizek.TestCase) error {
		err := property(tc)
		if err != nil && !isZizekControl(err) {
			once.Do(func() { t.Logf("first failure: %v\nnotes: %v", err, tc.Notes()) })
		}
		return err
	}
}

// isZizekControl reports whether err is zizek's own control flow (an
// example running out of choices), not a property failure.
func isZizekControl(err error) bool {
	var overrun *zizek.ReplayOverrun
	var invalid *zizek.InvalidReplay
	msg := err.Error()
	return errors.As(err, &overrun) || errors.As(err, &invalid) ||
		strings.HasPrefix(msg, "zizek:") || strings.Contains(msg, "choice budget")
}

// sharedServer is one JetStream server per stateful test; every example
// gets its own proxy, connections, stream and consumer.
type sharedServer struct {
	srv *harness.Server
	n   atomic.Int64
}

func (s *sharedServer) env(t testing.TB) *env {
	i := s.n.Add(1)
	return newEnvOn(t, s.srv, fmt.Sprintf("S%d", i), fmt.Sprintf("C%d", i))
}

// draw is a small helper around tc.Integer.
func draw(tc *zizek.TestCase, lo, hi int64) (int, error) {
	v, err := tc.Integer(lo, hi, lo)
	return int(v), err
}

// checkWire verifies BatchWithinMax / NoInvalidPull (and, with StopAfter,
// that no single pull asks for more than StopAfter) on every pull request
// the client put on the wire.
func checkWire(e *env, maxMsgs, stopAfter int) error {
	for _, b := range e.px.PullBatches() {
		if b < 1 || b > maxMsgs {
			return fmt.Errorf("BatchWithinMax/NoInvalidPull: pull request for %d messages with MaxMessages %d (all: %v)",
				b, maxMsgs, e.px.PullBatches())
		}
		if stopAfter > 0 && b > stopAfter {
			return fmt.Errorf("StopAfterRespected: pull request for %d messages with StopAfter %d (all: %v)",
				b, stopAfter, e.px.PullBatches())
		}
	}
	return nil
}

// lateHeartbeats drops idle heartbeats (and 408s) for a little more than
// 2x the heartbeat, so the client's heartbeat timer runs out while the
// server still holds its pull.
func lateHeartbeats(e *env, drop *atomic.Bool) {
	drop.Store(true)
	time.Sleep(2*stHeartbeat + 300*time.Millisecond)
	drop.Store(false)
}

// ---------------------------------------------------------------------
// Consume

type consumeState struct {
	t         testing.TB
	e         *env
	maxMsgs   int
	stopAfter int
	cc        jetstream.ConsumeContext
	base      int
	drop      atomic.Bool

	calls     atomic.Int64
	published int
	mu        sync.Mutex
	errs      []error
	faults    bool // a fault rule ran since the last settle
}

func (s *consumeState) takeErrs() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	errs := s.errs
	s.errs = nil
	return errs
}

func (s *consumeState) publish(tc *zizek.TestCase) error {
	n, err := draw(tc, 1, int64(2*s.maxMsgs))
	if err != nil {
		return err
	}
	s.e.publish(n)
	s.published += n
	tc.Note(fmt.Sprintf("publish %d", n))
	return nil
}

func (s *consumeState) cut(tc *zizek.TestCase) error {
	pulls := len(s.e.px.PullBatches())
	s.e.cutAndWaitReconnect()
	s.faults = true
	tc.Note("cut + reconnect")
	// Settle: the status goroutine re-pulls after CONNECTED (unless the
	// consume is done); wait for it so that the invariants see it.
	harness.Eventually(300*time.Millisecond, func() bool { return len(s.e.px.PullBatches()) > pulls })
	return nil
}

func (s *consumeState) late(tc *zizek.TestCase) error {
	lateHeartbeats(s.e, &s.drop)
	s.faults = true
	tc.Note("late heartbeats")
	return nil
}

func (s *consumeState) wait(tc *zizek.TestCase) error {
	time.Sleep(stExpiry + 200*time.Millisecond)
	tc.Note("wait for expiry")
	return nil
}

func (s *consumeState) expected() int64 {
	if s.stopAfter > 0 && s.published > s.stopAfter {
		return int64(s.stopAfter)
	}
	return int64(s.published)
}

// settle is the NoSilentStall probe: on a healthy connection every
// published message is delivered promptly, and without a fault since the
// last probe no error is reported.
func (s *consumeState) settle(tc *zizek.TestCase) error {
	if s.stopAfter > 0 && s.published >= s.stopAfter {
		tc.Assume(false)
	}
	s.e.publish(1)
	s.published++
	tc.Note("settle")
	if !harness.Eventually(700*time.Millisecond, func() bool { return s.calls.Load() >= s.expected() }) {
		return fmt.Errorf("NoSilentStall: %d of %d messages delivered 700ms after publishing on a healthy connection (pulls: %v)",
			s.calls.Load(), s.expected(), s.e.px.PullBatches())
	}
	errs := s.takeErrs()
	if !s.faults && len(errs) > 0 {
		return fmt.Errorf("NoSilentStall: errors reported on a healthy connection: %v", errs)
	}
	s.faults = false
	return nil
}

func (s *consumeState) invariants(_ *zizek.TestCase) error {
	if s.stopAfter > 0 && s.calls.Load() > int64(s.stopAfter) {
		return fmt.Errorf("StopAfterRespected: handler called %d times with StopAfter %d", s.calls.Load(), s.stopAfter)
	}
	return checkWire(s.e, s.maxMsgs, s.stopAfter)
}

func (s *consumeState) teardown(tc *zizek.TestCase) error {
	drain, err := tc.Boolean()
	if err != nil {
		return err
	}
	if drain {
		s.cc.Drain()
	} else {
		s.cc.Stop()
	}
	tc.Note(fmt.Sprintf("teardown drain=%v", drain))
	if !harness.Eventually(5*time.Second, func() bool { return consumeGoroutines() <= s.base }) {
		return fmt.Errorf("NoDeadlock: %d consume goroutine(s) still running 5s after Stop/Drain",
			consumeGoroutines()-s.base)
	}
	if !closedWithin(s.cc, 5*time.Second) {
		return errors.New("NoDeadlock: Closed() not closed 5s after Stop/Drain")
	}
	return s.invariants(tc)
}

func TestModelConsume(t *testing.T) {
	shared := &sharedServer{srv: harness.RunServer(t, true)}
	known := knownBugsMode()
	property := func(tc *zizek.TestCase) error {
		maxMsgs, err := draw(tc, 2, 6)
		if err != nil {
			return err
		}
		stopAfter := 0
		if known { // PULL-3, PULL-4
			if stopAfter, err = draw(tc, 0, int64(maxMsgs)); err != nil {
				return err
			}
		}
		tc.Note(fmt.Sprintf("Consume MaxMessages=%d StopAfter=%d", maxMsgs, stopAfter))
		e := shared.env(t)
		defer e.close()
		s := &consumeState{t: t, e: e, maxMsgs: maxMsgs, stopAfter: stopAfter, base: consumeGoroutines()}
		e.px.SetFilter(func(f *Frame) Verdict {
			if s.drop.Load() && (isStatus(f, "100") || isStatus(f, "408")) {
				return Drop
			}
			return Pass
		})
		opts := []jetstream.PullConsumeOpt{
			jetstream.PullMaxMessages(maxMsgs),
			jetstream.PullExpiry(stExpiry),
			jetstream.PullHeartbeat(stHeartbeat),
			jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
				s.mu.Lock()
				s.errs = append(s.errs, err)
				s.mu.Unlock()
			}),
		}
		if stopAfter > 0 {
			opts = append(opts, jetstream.StopAfter(stopAfter))
		}
		s.cc, err = e.cons.Consume(func(m jetstream.Msg) {
			s.calls.Add(1)
			_ = m.Ack()
		}, opts...)
		if err != nil {
			return err
		}
		machine := zizek.Machine[consumeState]{
			Rules: []zizek.Rule[consumeState]{
				{Name: "publish", Apply: (*consumeState).publish},
				{Name: "cut", Apply: (*consumeState).cut},
				{Name: "lateHeartbeats", Apply: (*consumeState).late},
				{Name: "wait", Apply: (*consumeState).wait},
				{Name: "settle", Apply: (*consumeState).settle},
			},
			Invariants: []zizek.Rule[consumeState]{
				{Name: "invariants", Apply: (*consumeState).invariants},
			},
		}
		if err := zizek.RunSteps(s, machine, tc, 8); err != nil {
			s.cc.Stop()
			return err
		}
		return s.teardown(tc)
	}
	cfg := zizek.Config{Examples: 6, Seed: 7, MaxShrinks: 8, MaxStall: 4, SuppressTooSlow: true}
	if err := zizek.Check(logFirst(t, property), cfg); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------
// Messages

type messagesState struct {
	t       testing.TB
	e       *env
	maxMsgs int
	it      jetstream.MessagesContext
	base    int
	drop    atomic.Bool
	known   bool

	slow      atomic.Int64
	published int
	returned  int
	faults    bool
}

// buffered is what the server delivered to the iterator (seen on the
// wire) minus what Next returned.
func (s *messagesState) buffered() int {
	return len(s.e.px.Frames(isStreamMsg)) - s.returned
}

func (s *messagesState) publish(tc *zizek.TestCase) error {
	n, err := draw(tc, 1, int64(2*s.maxMsgs))
	if err != nil {
		return err
	}
	s.e.publish(n)
	s.published += n
	tc.Note(fmt.Sprintf("publish %d", n))
	return nil
}

func (s *messagesState) next(tc *zizek.TestCase) error {
	k, err := draw(tc, 1, int64(s.maxMsgs))
	if err != nil {
		return err
	}
	tc.Assume(s.returned < s.published)
	got := 0
	for got < k && s.returned < s.published {
		_, err := s.it.Next(jetstream.NextMaxWait(time.Second))
		if err != nil {
			if !s.faults || !errors.Is(err, jetstream.ErrNoHeartbeat) {
				return fmt.Errorf("Next: %v", err)
			}
			continue
		}
		got++
		s.returned++
	}
	tc.Note(fmt.Sprintf("next x%d", got))
	return nil
}

func (s *messagesState) cut(tc *zizek.TestCase) error {
	// PULL-5: every reset of the pending count on a reconnect can over-pull
	// and overflow the channel (even with no backlog: the errConnected is
	// stale once Next has pulled again), and two unread reconnects before a
	// Drain trigger PULL-1. Only explored with MODELTEST_KNOWN_BUGS=1.
	tc.Assume(s.known)
	s.e.cutAndWaitReconnect()
	s.faults = true
	tc.Note("cut + reconnect")
	return nil
}

func (s *messagesState) late(tc *zizek.TestCase) error {
	// A missed heartbeat resets the pending count like a reconnect does
	// (PULL-5, PULL-6). Only explored with MODELTEST_KNOWN_BUGS=1.
	tc.Assume(s.known)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := s.it.Next(jetstream.NextMaxWait(3 * time.Second)); err == nil {
			s.returned++
		}
	}()
	lateHeartbeats(s.e, &s.drop)
	<-done
	s.faults = true
	tc.Note("late heartbeats")
	return nil
}

func (s *messagesState) wait(tc *zizek.TestCase) error {
	time.Sleep(stExpiry + 200*time.Millisecond)
	tc.Note("wait for expiry")
	return nil
}

func (s *messagesState) settle(tc *zizek.TestCase) error {
	s.e.publish(1)
	s.published++
	tc.Note("settle")
	for s.returned < s.published {
		start := time.Now()
		_, err := s.it.Next(jetstream.NextMaxWait(time.Second))
		if err != nil {
			return fmt.Errorf("NoSilentStall: Next on a healthy connection: %v (%d of %d returned, pulls: %v)",
				err, s.returned, s.published, s.e.px.PullBatches())
		}
		s.returned++
		if d := time.Since(start); d > 700*time.Millisecond {
			return fmt.Errorf("NoSilentStall: Next took %v on a healthy connection (pulls: %v)", d, s.e.px.PullBatches())
		}
	}
	s.faults = false
	return nil
}

func (s *messagesState) invariants(_ *zizek.TestCase) error {
	if n := s.slow.Load(); n > 0 {
		return fmt.Errorf("NoSlowConsumerDrop: %d message(s) dropped by the iterator's channel (pulls: %v)",
			n, s.e.px.PullBatches())
	}
	return checkWire(s.e, s.maxMsgs, 0)
}

func (s *messagesState) teardown(tc *zizek.TestCase) error {
	drain, err := tc.Boolean()
	if err != nil {
		return err
	}
	tc.Note(fmt.Sprintf("teardown drain=%v", drain))
	if !drain {
		s.it.Stop()
	} else {
		buffered := s.buffered()
		s.it.Drain()
		res := make(chan error, 1)
		got := 0
		go func() {
			for {
				if _, err := s.it.Next(); err != nil {
					res <- err
					return
				}
				got++
			}
		}()
		select {
		case err := <-res:
			if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				return fmt.Errorf("Next after Drain: %v", err)
			}
			if got < buffered {
				return fmt.Errorf("DrainKeepsBuffered: %d buffered message(s), %d returned after Drain", buffered, got)
			}
		case <-time.After(5 * time.Second):
			return errors.New("NoDeadlock: Next did not return 5s after Drain")
		}
	}
	if !harness.Eventually(5*time.Second, func() bool { return consumeGoroutines() <= s.base }) {
		return fmt.Errorf("NoDeadlock: %d iterator goroutine(s) still running 5s after Stop/Drain",
			consumeGoroutines()-s.base)
	}
	return s.invariants(tc)
}

func TestModelMessages(t *testing.T) {
	shared := &sharedServer{srv: harness.RunServer(t, true)}
	known := knownBugsMode()
	property := func(tc *zizek.TestCase) error {
		maxMsgs, err := draw(tc, 2, 6)
		if err != nil {
			return err
		}
		tc.Note(fmt.Sprintf("Messages MaxMessages=%d", maxMsgs))
		e := shared.env(t)
		defer e.close()
		s := &messagesState{t: t, e: e, maxMsgs: maxMsgs, base: consumeGoroutines(), known: known}
		e.nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if errors.Is(err, nats.ErrSlowConsumer) {
				s.slow.Add(1)
			}
		})
		e.px.SetFilter(func(f *Frame) Verdict {
			if s.drop.Load() && (isStatus(f, "100") || isStatus(f, "408")) {
				return Drop
			}
			return Pass
		})
		s.it, err = e.cons.Messages(
			jetstream.PullMaxMessages(maxMsgs),
			jetstream.PullExpiry(stExpiry),
			jetstream.PullHeartbeat(stHeartbeat))
		if err != nil {
			return err
		}
		machine := zizek.Machine[messagesState]{
			Rules: []zizek.Rule[messagesState]{
				{Name: "publish", Apply: (*messagesState).publish},
				{Name: "next", Apply: (*messagesState).next},
				{Name: "cut", Apply: (*messagesState).cut},
				{Name: "lateHeartbeats", Apply: (*messagesState).late},
				{Name: "wait", Apply: (*messagesState).wait},
				{Name: "settle", Apply: (*messagesState).settle},
			},
			Invariants: []zizek.Rule[messagesState]{
				{Name: "invariants", Apply: (*messagesState).invariants},
			},
		}
		if err := zizek.RunSteps(s, machine, tc, 8); err != nil {
			s.it.Stop()
			return err
		}
		return s.teardown(tc)
	}
	cfg := zizek.Config{Examples: 8, Seed: 11, MaxShrinks: 8, MaxStall: 4, SuppressTooSlow: true}
	if err := zizek.Check(logFirst(t, property), cfg); err != nil {
		t.Fatal(err)
	}
}
