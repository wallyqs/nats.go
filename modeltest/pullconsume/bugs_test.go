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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/modeltest/harness"
)

// Reproducers for the findings in models/PullConsume/README.md. Most replay
// the counterexample trace of the matching *_bug.cfg (named in each
// comment) against the real library. All of them assert the correct
// behavior, so they fail until the bug is fixed (harness.KnownBug skips
// them by default).
//
// Where a trace needs two goroutines to interleave in one particular way,
// the test holds the pullSubscription's own mutex (ConsumeContext is a
// *pullSubscription, which embeds sync.Mutex) to park them at the
// statements the trace names, and waits on goroutine stacks to know they
// got there. That only delays goroutines; it creates no state the library
// cannot reach by itself.

// PULL-1: a missed heartbeat makes the status goroutine queue a new pull on
// fetchNext while the 408 of the old pull is in flight. If Stop is called at
// that moment pullMessages returns, and the second of the two fetchNext
// sends (handler's checkPending, pull.go:488, or the status goroutine,
// pull.go:401) blocks forever holding sub.Lock. The subscription closed
// handler then blocks on sub.Lock as well, so Closed() never fires (and
// Closed() itself blocks).
//
// Trace: PullConsume_orphan_bug.cfg.
func TestPULL1StopWhileRepullQueuedDeadlocks(t *testing.T) {
	e := newEnv(t)
	// Missed heartbeats: drop idle heartbeats; hold the 408 of the first pull.
	e.px.SetFilter(func(f *Frame) Verdict {
		switch {
		case isStatus(f, "100"):
			return Drop
		case isStatus(f, "408"):
			return Hold
		}
		return Pass
	})
	errs := make(chan error, 100)
	base := consumeGoroutines()
	cc, err := e.cons.Consume(func(jetstream.Msg) {},
		jetstream.PullMaxMessages(10),
		jetstream.PullExpiry(time.Second),
		jetstream.PullHeartbeat(500*time.Millisecond),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) { errs <- err }))
	if err != nil {
		t.Fatal(err)
	}
	subLock := cc.(sync.Locker)
	subLock.Lock()
	// The heartbeat timer fires (2x500ms): the status goroutine takes the
	// ErrNoHeartbeat and waits for sub.Lock (pull.go:395 on the base).
	waitGoroutineAt(t, 10*time.Second, statusGoroutine, "Mutex).Lock")
	// The first pull expires at the server; let its 408 through. The
	// handler waits for sub.Lock (pull.go:262 on the base).
	if !harness.Eventually(10*time.Second, func() bool { return e.px.Held() > 0 }) {
		t.Fatal("no 408")
	}
	e.px.Release()
	waitGoroutineAt(t, 10*time.Second, consumeHandler, "Mutex).Lock")
	// Stop: pullMessages returns on done and unsubscribes.
	subs := e.nc.NumSubscriptions()
	cc.Stop()
	if !harness.Eventually(10*time.Second, func() bool { return e.nc.NumSubscriptions() == subs-1 }) {
		t.Fatal("pullMessages did not clean up")
	}
	subLock.Unlock()

	// Every goroutine of the consume must return (model: NoDeadlock /
	// NoOrphanedFetchSend). Closed() alone is not a reliable witness: it
	// reports closed as soon as the subscription is invalid (PULL-7).
	if !harness.Eventually(5*time.Second, func() bool { return consumeGoroutines() == base }) {
		t.Fatalf("%d consume goroutine(s) still running 5s after Stop; %d blocked forever on fetchNext holding sub.Lock",
			consumeGoroutines()-base, goroutinesAt("nats.go/jetstream.", "[chan send"))
	}
	if !closedWithin(cc, 5*time.Second) {
		t.Fatal("Closed() not closed after Stop")
	}
}

// PULL-2 (nats.go core, sendStatusEvent nats.go:6833-6851): the probe that
// is meant to detect a closed listener channel (`case <-ch:`) also succeeds
// when the channel merely holds an unread event. That event is consumed and
// lost, the listener is unregistered for the status being sent, and the
// status itself is not sent. Two status changes in a row before the
// listener reads the first one lose both, and the listener never hears that
// status again.
func TestPULL2StatusChangedLosesEvents(t *testing.T) {
	harness.KnownBug(t, "PULL-2")
	e := newEnv(t)
	ch := e.nc.StatusChanged(nats.CONNECTED, nats.RECONNECTING)
	e.cutAndWaitReconnect() // RECONNECTING, CONNECTED, nobody reading yet
	e.cutAndWaitReconnect()
	var got []nats.Status
	for len(ch) > 0 {
		got = append(got, <-ch)
	}
	want := []nats.Status{nats.RECONNECTING, nats.CONNECTED, nats.RECONNECTING, nats.CONNECTED}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("status events: got %v, want %v", got, want)
	}
}

// PULL-2, consequence for Consume: if the status goroutine is busy (here in
// a ConsumeErrHandler that takes a while) when a reconnect happens, the
// CONNECTED listener is dropped. On the next reconnect the status goroutine
// sees RECONNECTING (stops the heartbeat timer) but never CONNECTED, so it
// never re-issues the pull and never re-arms the timer. If the server lost
// the pending pull (restart), the consumer goes silent for good on a
// healthy connection, with no error reported.
//
// Trace: PullConsume_steal_bug.cfg (NoPermanentStall).
func TestPULL2ConsumeStallsForeverAfterLostConnected(t *testing.T) {
	harness.KnownBug(t, "PULL-2")
	e := newEnv(t)
	var dropStatus atomic.Bool
	e.px.SetFilter(func(f *Frame) Verdict {
		if dropStatus.Load() && (isStatus(f, "100") || isStatus(f, "408")) {
			return Drop
		}
		return Pass
	})
	var received atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	cc, err := e.cons.Consume(func(m jetstream.Msg) {
		received.Add(1)
		m.Ack()
	},
		jetstream.PullExpiry(time.Second),
		jetstream.PullHeartbeat(500*time.Millisecond),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			once.Do(func() {
				close(entered)
				<-release // a slow error handler
			})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Stop()
	// Missed heartbeats -> ErrNoHeartbeat -> the error handler is running.
	dropStatus.Store(true)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("no ErrNoHeartbeat")
	}
	dropStatus.Store(false)
	// A reconnect while the status goroutine is in the error handler.
	e.cutAndWaitReconnect()
	close(release)
	// The server restarts: every waiting pull request is gone.
	before := e.reconnects()
	e.srv.Restart()
	if !harness.Eventually(10*time.Second, func() bool { return e.reconnects() > before && e.nc.IsConnected() }) {
		t.Fatal("no reconnect after restart")
	}
	if !harness.Eventually(10*time.Second, func() bool { return e.pubNC.IsConnected() }) {
		t.Fatal("publisher did not reconnect")
	}
	e.publish(3)
	if !harness.Eventually(10*time.Second, func() bool { return received.Load() == 3 }) {
		t.Fatalf("consumer stalled: received %d of 3 messages within 10s (expiry 1s) on a healthy connection; pulls on the wire: %v",
			received.Load(), e.px.PullBatches())
	}
}

// PULL-3: after a reconnect the status goroutine re-pulls a full
// MaxMessages batch (pull.go:373-383) without applying StopAfter, and the
// handler keeps getting messages that were already queued when StopAfter
// stopped the consume (pull.go:286-301 never checks closed). The handler
// runs more than StopAfter times.
//
// Trace: PullConsume_stopafter_bug.cfg (StopAfterRespected).
func TestPULL3StopAfterExceededAfterReconnect(t *testing.T) {
	e := newEnv(t)
	const stopAfter = 3
	var calls atomic.Int64
	cc, err := e.cons.Consume(func(m jetstream.Msg) {
		calls.Add(1)
		m.Ack()
		time.Sleep(10 * time.Millisecond) // the rest of the batch queues up meanwhile
	}, jetstream.StopAfter(stopAfter), jetstream.PullMaxMessages(10))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Stop()
	if !harness.Eventually(5*time.Second, func() bool { return len(e.px.PullBatches()) == 1 }) {
		t.Fatal("no initial pull")
	}
	e.cutAndWaitReconnect()
	if !harness.Eventually(5*time.Second, func() bool { return len(e.px.PullBatches()) == 2 }) {
		t.Fatal("no pull after reconnect")
	}
	e.publish(10)
	if !closedWithin(cc, 10*time.Second) {
		t.Fatal("consume did not stop")
	}
	time.Sleep(100 * time.Millisecond) // let a straggling callback finish
	batches := e.px.PullBatches()
	for _, b := range batches {
		if b > stopAfter {
			t.Errorf("pull request for %d messages with StopAfter(%d): %v", b, stopAfter, batches)
		}
	}
	if n := calls.Load(); n > stopAfter {
		t.Errorf("handler called %d times with StopAfter(%d)", n, stopAfter)
	}
}

// PULL-4: with StopAfter < MaxMessages the pending count is reset to
// MaxMessages although the pull asked for fewer (pull.go:328-332, 500,
// 415), so after a pull expires partly unfilled the 408 never brings the
// count below the threshold: no new pull is issued. Messages(): Next fails
// with ErrNoHeartbeat 2 heartbeats later instead of returning the next
// message; Consume: ErrNoHeartbeat is reported and new messages wait for
// the heartbeat timeout.
//
// Trace: PullConsume_pendingreset_bug.cfg (NoSilentStall).
func TestPULL4MessagesStopAfterStallsAfterExpiry(t *testing.T) {
	e := newEnv(t)
	e.publish(1)
	it, err := e.cons.Messages(jetstream.StopAfter(5), jetstream.PullExpiry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer it.Stop()
	if _, err := it.Next(jetstream.NextMaxWait(5 * time.Second)); err != nil {
		t.Fatalf("first Next: %v", err)
	}
	// The first pull (4 messages left) expires unfilled.
	if !harness.Eventually(5*time.Second, func() bool {
		return len(e.px.Frames(func(f *Frame) bool { return isStatus(f, "408") })) > 0
	}) {
		t.Fatal("no 408")
	}
	e.publish(1)
	if _, err := it.Next(jetstream.NextMaxWait(5 * time.Second)); err != nil {
		t.Fatalf("second Next after the first pull expired: %v (pulls on the wire: %v)", err, e.px.PullBatches())
	}
}

func TestPULL4ConsumeStopAfterStallsAfterExpiry(t *testing.T) {
	e := newEnv(t)
	var calls atomic.Int64
	errs := make(chan error, 10)
	cc, err := e.cons.Consume(func(m jetstream.Msg) {
		calls.Add(1)
		m.Ack()
	}, jetstream.StopAfter(5), jetstream.PullExpiry(time.Second),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) { errs <- err }))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Stop()
	if !harness.Eventually(5*time.Second, func() bool {
		return len(e.px.Frames(func(f *Frame) bool { return isStatus(f, "408") })) > 0
	}) {
		t.Fatal("no 408")
	}
	start := time.Now()
	e.publish(1)
	// A pull should be waiting at the server: the message arrives at once.
	if !harness.Eventually(5*time.Second, func() bool { return calls.Load() == 1 }) {
		t.Fatal("message not delivered")
	}
	if d := time.Since(start); d > 250*time.Millisecond {
		t.Errorf("message delivered after %v: no pull was outstanding after the first one expired", d)
	}
	select {
	case err := <-errs:
		t.Errorf("error reported on a healthy connection: %v", err)
	default:
	}
}

// PULL-7: Closed() (pull.go:846-849) returns a closed channel as soon as
// the subscription is no longer valid, which Stop's Unsubscribe makes true
// while the message handler may still be running. Its contract is "When the
// channel is closed, no more messages will be received and processing is
// complete."
func TestPULL7ClosedBeforeHandlerReturns(t *testing.T) {
	e := newEnv(t)
	e.publish(1)
	entered := make(chan struct{})
	release := make(chan struct{})
	subs := e.nc.NumSubscriptions()
	cc, err := e.cons.Consume(func(m jetstream.Msg) {
		close(entered)
		<-release
	})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	cc.Stop()
	if !harness.Eventually(5*time.Second, func() bool { return e.nc.NumSubscriptions() == subs }) {
		t.Fatal("not unsubscribed")
	}
	closedEarly := closedWithin(cc, 500*time.Millisecond)
	close(release)
	if closedEarly {
		t.Fatal("Closed() is closed while the message handler is still running")
	}
	if !closedWithin(cc, 5*time.Second) {
		t.Fatal("Closed() not closed after the handler returned")
	}
}

// PULL-5: Messages() buffers in a ChanSubscription whose channel holds
// exactly MaxMessages (pull.go:534); nats.go drops what does not fit as a
// slow consumer (nats.go:4013-4022). After a reconnect Next resets its
// pending count to 0 (pull.go:729) and pulls a full MaxMessages batch
// although the channel may still hold a backlog (and earlier pulls may
// still be alive at the server or in the reconnect buffer), so the new
// batch does not fit and messages are dropped.
//
// Trace: PullMessages_overflow_bug.cfg (NoSlowConsumerDrop).
func TestPULL5MessagesOverflowAfterReconnect(t *testing.T) {
	e := newEnv(t)
	var slow atomic.Int64
	e.nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		if errors.Is(err, nats.ErrSlowConsumer) {
			slow.Add(1)
		}
	})
	const max = 10
	e.publish(max)
	it, err := e.cons.Messages(jetstream.PullMaxMessages(max))
	if err != nil {
		t.Fatal(err)
	}
	defer it.Stop()
	if _, err := it.Next(); err != nil { // pulls 10; 9 stay buffered
		t.Fatal(err)
	}
	if !harness.Eventually(5*time.Second, func() bool {
		return len(e.px.Frames(isStreamMsg)) == max
	}) {
		t.Fatal("first batch not delivered")
	}
	e.cutAndWaitReconnect()
	e.publish(2 * max)
	// Drain the iterator at an ordinary pace.
	got := 1
	for got < 3*max {
		if _, err := it.Next(jetstream.NextMaxWait(3 * time.Second)); err != nil {
			break
		}
		got++
		time.Sleep(5 * time.Millisecond)
	}
	if n := slow.Load(); n > 0 {
		t.Fatalf("%d slow consumer drop(s) on the iterator after a reconnect (pulls on the wire: %v, received %d of %d)",
			n, e.px.PullBatches(), got, 3*max)
	}
	if got != 3*max {
		t.Fatalf("received %d of %d messages (pulls on the wire: %v)", got, 3*max, e.px.PullBatches())
	}
}

// PULL-1, Messages() variant: during Drain Next keeps calling checkPending
// (pull.go:672) after pullMessages has returned. Every errConnected resets
// the pending count to 0 (pull.go:729), so the first resulting fetchNext
// send fills the buffer and the second blocks forever while Next holds
// sub.Lock: Next never returns, although Drain must end the iteration
// with ErrMsgIteratorClosed.
//
// Trace: PullMessages_orphan_bug.cfg (NoOrphanedFetchSend).
func TestPULL1MessagesNextHangsDuringDrain(t *testing.T) {
	e := newEnv(t)
	var holdPong atomic.Bool
	e.px.SetFilter(func(f *Frame) Verdict {
		if holdPong.Load() && f.ToClient && f.Op == "PONG" {
			return Hold // keeps checkDrained's Flush, and so the drain, pending
		}
		return Pass
	})
	e.publish(1)
	it, err := e.cons.Messages(jetstream.PullMaxMessages(10))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := it.Next(); err != nil { // first pull; pending 10 -> 9
		t.Fatal(err)
	}
	// Two reconnects while nobody is in Next: the status goroutine queues
	// errDisconnected, errConnected, errDisconnected, errConnected on errs.
	e.cutAndWaitReconnect()
	e.cutAndWaitReconnect()
	if !harness.Eventually(5*time.Second, func() bool { return chanLen(it, "errs") == 4 }) {
		t.Fatalf("expected 4 queued reconnect notifications, have %d", chanLen(it, "errs"))
	}
	holdPong.Store(true)
	it.Drain()
	res := make(chan error, 1)
	go func() {
		for {
			if _, err := it.Next(); err != nil {
				res <- err
				return
			}
		}
	}()
	select {
	case err := <-res:
		if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			t.Fatalf("Next after Drain: %v", err)
		}
	case <-time.After(5 * time.Second):
		holdPong.Store(false)
		e.px.Release() // let the drain complete: Next still does not return
		select {
		case <-res:
		case <-time.After(5 * time.Second):
			t.Fatalf("Next did not return after Drain: %d goroutine(s) blocked on fetchNext in checkPending holding sub.Lock",
				goroutinesAt("(*pullSubscription).checkPending", "[chan send"))
		}
	}
}

// PULL-9: FetchBytes asks for Batch = 1,000,000 (pull.go:890) and fetch
// sizes both of its channels by the batch (pull.go:935-937): one call
// allocates ~32 MB of channel buffers, whatever maxBytes is. Messages()
// with PullMaxBytes likewise allocates an 8 MB channel (pull.go:534,
// MaxMessages = 1,000,000 at pull.go:1222).
func TestPULL9FetchBytesAllocatesChannelsForAMillionMessages(t *testing.T) {
	harness.KnownBug(t, "PULL-9")
	e := newEnv(t)
	e.publish(1)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	batch, err := e.cons.FetchBytes(1024, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for range batch.Messages() {
	}
	runtime.ReadMemStats(&after)
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 4<<20 {
		t.Fatalf("FetchBytes(1024) allocated %d MB", alloc>>20)
	}
}
