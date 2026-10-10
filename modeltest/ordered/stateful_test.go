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

package ordered

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/modeltest/harness"
	zizek "github.com/wallyqs/zizek.go"
)

// Stateful tests derived from the models. Each rule is one environment or
// user action of the model (Deliver = publish, Disconnect/Reconnect = cut,
// server restart, ExtDelete = delete, Lose = steal) and each invariant is a
// model invariant checked against the real ordered consumer:
//
//   InOrder / NoDupNoGap  what the user got is exactly 1..k in stream order
//   SerialHandler         the Consume handler never runs twice at once
//   AllDelivered          "settle": everything published is eventually seen
//
// The rules that reproduced ORD-4 (slow handler + cut) and ORD-2
// (fetchNoWait) are part of the default machines since those were fixed.
// The Messages machine still issues the first pull before any rule runs,
// because a consumer lost while no pull waits is ORD-12 (open).

const settleTimeout = 45 * time.Second // > 2 x 15s default heartbeat (ORD-13)

// ruleCounts counts rule applications per test, logged at the end so a run
// shows what it exercised.
type ruleCounts struct {
	mu sync.Mutex
	n  map[string]int
}

func (r *ruleCounts) wrap(name string) {
	r.mu.Lock()
	if r.n == nil {
		r.n = map[string]int{}
	}
	r.n[name]++
	r.mu.Unlock()
}

func counted[S any](rc *ruleCounts, rules []zizek.Rule[S]) []zizek.Rule[S] {
	out := make([]zizek.Rule[S], len(rules))
	for i, r := range rules {
		apply, name := r.Apply, r.Name
		out[i] = zizek.Rule[S]{Name: name, Apply: func(s *S, tc *zizek.TestCase) error {
			rc.wrap(name)
			return apply(s, tc)
		}}
	}
	return out
}

// seqLog records the stream sequences the user received.
type seqLog struct {
	mu          sync.Mutex
	got         []uint64
	inflight    atomic.Int32
	maxInflight atomic.Int32
}

func (l *seqLog) add(s uint64) {
	l.mu.Lock()
	l.got = append(l.got, s)
	l.mu.Unlock()
}

func (l *seqLog) snapshot() []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]uint64(nil), l.got...)
}

// seqOf is the stream sequence of m, 0 if its metadata cannot be parsed
// (which then fails InOrder).
func seqOf(m jetstream.Msg) uint64 {
	md, err := m.Metadata()
	if err != nil {
		return 0
	}
	return md.Sequence.Stream
}

func inOrder(got []uint64) error {
	for i, s := range got {
		if s != uint64(i+1) {
			return fmt.Errorf("user got stream sequences %v, want 1..%d in order", got, len(got))
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Consume
// ---------------------------------------------------------------------------

type consumeState struct {
	t   *testing.T
	e   *env
	c   jetstream.Consumer
	cc  jetstream.ConsumeContext
	log seqLog
}

func (s *consumeState) publish(tc *zizek.TestCase) error {
	n, err := tc.Integer(1, 3, 1)
	if err != nil {
		return err
	}
	s.e.publish(int(n))
	tc.Note(fmt.Sprintf("publish %d (stream at %d)", n, s.e.seq))
	return nil
}

// cut drops the client's TCP connection; it reconnects at once
// (Disconnect + Reconnect, the reset trigger errConnected).
func (s *consumeState) cut(tc *zizek.TestCase) error {
	s.e.proxy.Cut()
	tc.Note("cut connection")
	return nil
}

// restart restarts the server; the memory-backed consumer is gone after it.
func (s *consumeState) restart(tc *zizek.TestCase) error {
	s.e.srv.Restart()
	tc.Note("restart server")
	return nil
}

// deleteConsumer deletes the current server-side consumer (ExtDelete).
func (s *consumeState) deleteConsumer(tc *zizek.TestCase) error {
	info := s.c.CachedInfo()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.e.ajs.DeleteConsumer(ctx, streamName, info.Name)
	tc.Note(fmt.Sprintf("delete consumer %s: %v", info.Name, err))
	return nil
}

// steal takes the next message of the current server-side consumer with a
// separate pull, so the ordered consumer sees a gap in the consumer
// sequence (Lose). Two messages are published afterwards so that the gap
// is followed by a message (a gap at the tail is ORD-10).
func (s *consumeState) steal(tc *zizek.TestCase) error {
	info := s.c.CachedInfo()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stolen := 0
	if cons, err := s.e.ajs.Consumer(ctx, streamName, info.Name); err == nil {
		if b, err := cons.Fetch(1, jetstream.FetchMaxWait(200*time.Millisecond)); err == nil {
			for range b.Messages() {
				stolen++
			}
		}
	}
	s.e.publish(2)
	tc.Note(fmt.Sprintf("steal from %s: %d message(s); publish 2 (stream at %d)", info.Name, stolen, s.e.seq))
	return nil
}

// settle checks AllDelivered: everything published reaches the handler.
func (s *consumeState) settle(tc *zizek.TestCase) error {
	want := s.e.seq
	ok := harness.Eventually(settleTimeout, func() bool { return uint64(len(s.log.snapshot())) >= want })
	tc.Note(fmt.Sprintf("settle at %d", want))
	if !ok {
		return fmt.Errorf("after %v the handler got %v of 1..%d", settleTimeout, s.log.snapshot(), want)
	}
	return nil
}

func (s *consumeState) invInOrder(_ *zizek.TestCase) error { return inOrder(s.log.snapshot()) }

func (s *consumeState) invSerial(_ *zizek.TestCase) error {
	if n := s.log.maxInflight.Load(); n > 1 {
		return fmt.Errorf("handler ran for %d messages at once", n)
	}
	return nil
}

func (s *consumeState) handler(m jetstream.Msg) {
	n := s.log.inflight.Add(1)
	for {
		old := s.log.maxInflight.Load()
		if n <= old || s.log.maxInflight.CompareAndSwap(old, n) {
			break
		}
	}
	seq := seqOf(m)
	hookMu.Lock()
	h := hook
	hookMu.Unlock()
	if h != nil {
		h(seq)
	}
	s.log.add(seq)
	s.log.inflight.Add(-1)
}

func runConsumeMachine(t *testing.T, extra []zizek.Rule[consumeState], examples int) {
	var rc ruleCounts
	defer func() { t.Logf("rules applied: %v", rc.n) }()
	prop := func(tc *zizek.TestCase) error {
		st := &consumeState{t: t, e: newEnv(t, envOpts{proxy: true})}
		defer st.e.close()
		st.c = st.e.ordered(jetstream.OrderedConsumerConfig{})
		var err error
		st.cc, err = st.c.Consume(st.handler)
		if err != nil {
			return err
		}
		defer st.cc.Stop()
		rules := append([]zizek.Rule[consumeState]{
			{Name: "publish", Apply: (*consumeState).publish},
			{Name: "cut", Apply: (*consumeState).cut},
			{Name: "restart", Apply: (*consumeState).restart},
			{Name: "deleteConsumer", Apply: (*consumeState).deleteConsumer},
			{Name: "steal", Apply: (*consumeState).steal},
			{Name: "settle", Apply: (*consumeState).settle},
		}, extra...)
		m := zizek.Machine[consumeState]{
			Rules: counted(&rc, rules),
			Invariants: []zizek.Rule[consumeState]{
				{Name: "InOrder", Apply: (*consumeState).invInOrder},
				{Name: "SerialHandler", Apply: (*consumeState).invSerial},
			},
		}
		if err := zizek.RunSteps(st, m, tc, 10); err != nil {
			return err
		}
		return st.settle(tc)
	}
	if err := zizek.Check(prop, zizek.Config{Examples: examples, Seed: 7, SuppressTooSlow: true,
		MaxShrinks: 20}); err != nil {
		t.Fatal(err)
	}
}

// TestStatefulConsume includes a slow handler rule (ORD-4): message
// handling overlaps a reconnect.
func TestStatefulConsume(t *testing.T) {
	slowCut := zizek.Rule[consumeState]{Name: "slowHandlerCut", Apply: func(s *consumeState, tc *zizek.TestCase) error {
		// publish two messages and cut the connection while the handler of
		// the first one runs
		entered := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		want := s.e.seq + 1
		hookMu.Lock()
		hook = func(seq uint64) {
			if seq == want {
				once.Do(func() { close(entered) })
				<-release
			}
		}
		hookMu.Unlock()
		s.e.publish(2)
		if _, ok := recvWithin(entered, 10*time.Second); ok {
			s.e.proxy.Cut()
			time.Sleep(300 * time.Millisecond)
		}
		close(release)
		hookMu.Lock()
		hook = nil
		hookMu.Unlock()
		tc.Note("slow handler + cut")
		return nil
	}}
	runConsumeMachine(t, []zizek.Rule[consumeState]{slowCut}, 15)
}

// hook, when set, runs inside the Consume handler before the message is
// logged (used by the slow-handler rule).
var (
	hookMu sync.Mutex
	hook   func(seq uint64)
)

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

type messagesState struct {
	e   *env
	c   jetstream.Consumer
	it  jetstream.MessagesContext
	got []uint64
}

func (s *messagesState) publish(tc *zizek.TestCase) error {
	n, err := tc.Integer(1, 3, 1)
	if err != nil {
		return err
	}
	s.e.publish(int(n))
	tc.Note(fmt.Sprintf("publish %d (stream at %d)", n, s.e.seq))
	return nil
}

func (s *messagesState) cut(tc *zizek.TestCase) error {
	s.e.proxy.Cut()
	tc.Note("cut connection")
	return nil
}

func (s *messagesState) restart(tc *zizek.TestCase) error {
	s.e.srv.Restart()
	tc.Note("restart server")
	return nil
}

func (s *messagesState) deleteConsumer(tc *zizek.TestCase) error {
	info := s.c.CachedInfo()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.e.ajs.DeleteConsumer(ctx, streamName, info.Name)
	tc.Note(fmt.Sprintf("delete consumer %s: %v", info.Name, err))
	return nil
}

// next calls Next once with a short deadline (one step of the user's loop).
func (s *messagesState) next(tc *zizek.TestCase) error {
	m, err := s.it.Next(jetstream.NextMaxWait(300 * time.Millisecond))
	switch {
	case err == nil:
		s.got = append(s.got, seqOf(m))
		tc.Note(fmt.Sprintf("next: %d", s.got[len(s.got)-1]))
	case errors.Is(err, nats.ErrTimeout):
		tc.Note("next: timeout")
	default:
		return fmt.Errorf("next: %w", err)
	}
	return nil
}

// settle keeps calling Next (deadline above two default heartbeats) until
// everything published was returned.
func (s *messagesState) settle(tc *zizek.TestCase) error {
	deadline := time.Now().Add(settleTimeout)
	for uint64(len(s.got)) < s.e.seq && time.Now().Before(deadline) {
		m, err := s.it.Next(jetstream.NextMaxWait(time.Until(deadline)))
		if err != nil {
			return fmt.Errorf("settle: Next: %w (got %v of 1..%d)", err, s.got, s.e.seq)
		}
		s.got = append(s.got, seqOf(m))
		if err := inOrder(s.got); err != nil {
			return err
		}
	}
	tc.Note(fmt.Sprintf("settle at %d", s.e.seq))
	if uint64(len(s.got)) < s.e.seq {
		return fmt.Errorf("settle: got %v of 1..%d", s.got, s.e.seq)
	}
	return nil
}

func (s *messagesState) invInOrder(_ *zizek.TestCase) error { return inOrder(s.got) }

func TestStatefulMessages(t *testing.T) {
	var rc ruleCounts
	defer func() { t.Logf("rules applied: %v", rc.n) }()
	prop := func(tc *zizek.TestCase) error {
		st := &messagesState{e: newEnv(t, envOpts{proxy: true})}
		defer st.e.close()
		st.c = st.e.ordered(jetstream.OrderedConsumerConfig{})
		var err error
		if st.it, err = st.c.Messages(); err != nil {
			return err
		}
		defer st.it.Stop()
		// Issue the first pull now: a consumer lost while no pull waits is
		// ORD-12.
		if _, err := st.it.Next(jetstream.NextMaxWait(100 * time.Millisecond)); !errors.Is(err, nats.ErrTimeout) {
			return fmt.Errorf("priming Next: %v", err)
		}
		m := zizek.Machine[messagesState]{
			Rules: counted(&rc, []zizek.Rule[messagesState]{
				{Name: "publish", Apply: (*messagesState).publish},
				{Name: "cut", Apply: (*messagesState).cut},
				{Name: "restart", Apply: (*messagesState).restart},
				{Name: "deleteConsumer", Apply: (*messagesState).deleteConsumer},
				{Name: "next", Apply: (*messagesState).next},
				{Name: "settle", Apply: (*messagesState).settle},
			}),
			Invariants: []zizek.Rule[messagesState]{{Name: "InOrder", Apply: (*messagesState).invInOrder}},
		}
		if err := zizek.RunSteps(st, m, tc, 10); err != nil {
			return err
		}
		return st.settle(tc)
	}
	if err := zizek.Check(prop, zizek.Config{Examples: 15, Seed: 11, SuppressTooSlow: true, MaxShrinks: 20}); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

type fetchState struct {
	e   *env
	c   jetstream.Consumer
	got []uint64
}

func (s *fetchState) publish(tc *zizek.TestCase) error {
	n, err := tc.Integer(1, 4, 1)
	if err != nil {
		return err
	}
	s.e.publish(int(n))
	tc.Note(fmt.Sprintf("publish %d (stream at %d)", n, s.e.seq))
	return nil
}

func (s *fetchState) read(b jetstream.MessageBatch) {
	for m := range b.Messages() {
		s.got = append(s.got, seqOf(m))
	}
}

func (s *fetchState) fetch(tc *zizek.TestCase) error {
	n, err := tc.Integer(1, 4, 1)
	if err != nil {
		return err
	}
	b, err := s.c.Fetch(int(n), jetstream.FetchMaxWait(200*time.Millisecond))
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	s.read(b)
	tc.Note(fmt.Sprintf("fetch %d -> %v", n, s.got))
	return nil
}

func (s *fetchState) fetchBytes(tc *zizek.TestCase) error {
	b, err := s.c.FetchBytes(64, jetstream.FetchMaxWait(200*time.Millisecond))
	if err != nil {
		return fmt.Errorf("fetchBytes: %w", err)
	}
	s.read(b)
	tc.Note(fmt.Sprintf("fetchBytes -> %v", s.got))
	return nil
}

func (s *fetchState) next(tc *zizek.TestCase) error {
	m, err := s.c.Next(jetstream.FetchMaxWait(200 * time.Millisecond))
	if err == nil {
		s.got = append(s.got, seqOf(m))
	} else if !errors.Is(err, nats.ErrTimeout) {
		return fmt.Errorf("next: %w", err)
	}
	tc.Note(fmt.Sprintf("next -> %v", s.got))
	return nil
}

func (s *fetchState) fetchNoWait(tc *zizek.TestCase) error {
	n, err := tc.Integer(1, 4, 1)
	if err != nil {
		return err
	}
	b, err := s.c.FetchNoWait(int(n))
	if err != nil {
		return fmt.Errorf("fetchNoWait: %w", err)
	}
	s.read(b)
	tc.Note(fmt.Sprintf("fetchNoWait %d -> %v", n, s.got))
	return nil
}

func (s *fetchState) invInOrder(_ *zizek.TestCase) error { return inOrder(s.got) }

func TestStatefulFetch(t *testing.T) {
	var rc ruleCounts
	defer func() { t.Logf("rules applied: %v", rc.n) }()
	prop := func(tc *zizek.TestCase) error {
		st := &fetchState{e: newEnv(t, envOpts{})}
		defer st.e.close()
		st.c = st.e.ordered(jetstream.OrderedConsumerConfig{})
		rules := []zizek.Rule[fetchState]{
			{Name: "publish", Apply: (*fetchState).publish},
			{Name: "fetch", Apply: (*fetchState).fetch},
			{Name: "fetchBytes", Apply: (*fetchState).fetchBytes},
			{Name: "next", Apply: (*fetchState).next},
			{Name: "fetchNoWait", Apply: (*fetchState).fetchNoWait}, // ORD-2
		}
		m := zizek.Machine[fetchState]{
			Rules:      counted(&rc, rules),
			Invariants: []zizek.Rule[fetchState]{{Name: "InOrder", Apply: (*fetchState).invInOrder}},
		}
		return zizek.RunSteps(st, m, tc, 15)
	}
	if err := zizek.Check(prop, zizek.Config{Examples: 15, Seed: 3, SuppressTooSlow: true}); err != nil {
		t.Fatal(err)
	}
}
