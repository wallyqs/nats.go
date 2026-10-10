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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/modeltest/harness"
)

// Reproducers for the findings in models/OrderedConsumer/README.md. Each one
// replays the counterexample of the matching *_bug.cfg against the real
// library and asserts the correct behavior. Fixed findings run as regression
// tests; harness.KnownBug skips the ones that are still open (ORD-12) by
// default.
//
// Interleavings are forced, not waited for: the JetStream API relay holds
// the consumer create of reset() (the model's "create in flight" step), and
// the tests wait on goroutine stacks to know a goroutine reached the
// statement the trace names.

// ORD-1: Fetch returns ErrOrderedConsumerConcurrentRequests without
// releasing the ordered consumer's mutex (ordered.go:420-423, and
// FetchBytes at 460-463). Every later call that takes the mutex blocks
// forever. Trace: OrderedFetch_locked_bug.cfg.
func TestORD1FetchConcurrentRequestsLeaksLock(t *testing.T) {
	for _, name := range []string{"Fetch", "FetchBytes"} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, envOpts{})
			c := e.ordered(jetstream.OrderedConsumerConfig{})
			// Empty stream: the first fetch stays open for its MaxWait.
			res, err := c.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if name == "Fetch" {
				_, err = c.Fetch(1)
			} else {
				_, err = c.FetchBytes(100)
			}
			if !errors.Is(err, jetstream.ErrOrderedConsumerConcurrentRequests) {
				t.Fatalf("second fetch: got %v, want ErrOrderedConsumerConcurrentRequests", err)
			}
			for range res.Messages() {
			}
			done := make(chan struct{})
			go func() {
				c.CachedInfo()
				close(done)
			}()
			if _, ok := recvWithin(done, 5*time.Second); !ok {
				t.Fatal("CachedInfo blocked: the rejected fetch left the ordered consumer locked")
			}
		})
	}
}

// ORD-2: FetchNoWait neither records its result as the running fetch nor
// advances the cursor (ordered.go:495-513), so the next fetch starts the
// new consumer at the old cursor and hands out the same messages again.
// Trace: OrderedFetch_nowait_bug.cfg.
func TestORD2FetchNoWaitRedelivers(t *testing.T) {
	cases := []struct {
		name  string
		first func(jetstream.Consumer) (jetstream.MessageBatch, error)
	}{
		{"FetchNoWait-FetchNoWait", func(c jetstream.Consumer) (jetstream.MessageBatch, error) { return c.FetchNoWait(3) }},
		{"Fetch-FetchNoWait", func(c jetstream.Consumer) (jetstream.MessageBatch, error) {
			return c.Fetch(3, jetstream.FetchMaxWait(time.Second))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, envOpts{})
			e.publish(6)
			c := e.ordered(jetstream.OrderedConsumerConfig{})
			var got []uint64
			b, err := tc.first(c)
			if err != nil {
				t.Fatal(err)
			}
			for m := range b.Messages() {
				got = append(got, streamSeq(t, m))
			}
			b, err = c.FetchNoWait(3)
			if err != nil {
				t.Fatal(err)
			}
			for m := range b.Messages() {
				got = append(got, streamSeq(t, m))
			}
			for i, s := range got {
				if s != uint64(i+1) {
					t.Fatalf("handed out stream sequences %v, want 1..%d in order", got, len(got))
				}
			}
		})
	}
}

// ORD-3: errHandler calls the user's ConsumeErrHandler while holding the
// ordered consumer's mutex (ordered.go:206-209). Calling Stop on the
// ordered ConsumeContext (or CachedInfo/Info on the consumer) from the
// handler deadlocks the handler and, with it, the consume.
// Trace: ErrHandlerStop_bug.cfg.
func TestORD3ErrHandlerCallingStopDeadlocks(t *testing.T) {
	for _, call := range []string{"Stop", "CachedInfo"} {
		t.Run(call, func(t *testing.T) {
			e := newEnv(t, envOpts{})
			c := e.ordered(jetstream.OrderedConsumerConfig{})
			var ccp atomic.Pointer[jetstream.ConsumeContext]
			returned := make(chan struct{})
			var once sync.Once
			cc, err := c.Consume(func(m jetstream.Msg) {}, jetstream.ConsumeErrHandler(
				func(_ jetstream.ConsumeContext, err error) {
					if !errors.Is(err, jetstream.ErrConsumerDeleted) {
						return
					}
					if call == "Stop" {
						(*ccp.Load()).Stop()
					} else {
						c.CachedInfo()
					}
					once.Do(func() { close(returned) })
				}))
			if err != nil {
				t.Fatal(err)
			}
			ccp.Store(&cc)
			defer func() { go cc.Stop() }()
			// Let the inner subscription's pull request reach the server
			// so that the deletion answers it with 409 Consumer Deleted.
			time.Sleep(200 * time.Millisecond)
			e.deleteConsumer(consumerName(t, c))
			if _, ok := recvWithin(returned, 5*time.Second); !ok {
				t.Fatalf("ConsumeErrHandler calling %s did not return: deadlock on the ordered consumer's mutex", call)
			}
		})
	}
}

// ORD-4: a reconnect while the handler is running makes the reset goroutine
// start a new inner subscription right away (ordered.go:162-189); its
// messages reach the handler on another goroutine while the first call is
// still running, and can finish first. Trace: ConcurrentHandler_bug.cfg,
// OutOfOrder_bug.cfg.
func TestORD4HandlerRunsConcurrentlyAcrossReset(t *testing.T) {
	e := newEnv(t, envOpts{proxy: true})
	e.publish(3)
	c := e.ordered(jetstream.OrderedConsumerConfig{})
	var inflight, maxInflight atomic.Int32
	var mu sync.Mutex
	var finished []uint64
	release := make(chan struct{})
	first := make(chan struct{})
	cc, err := c.Consume(func(m jetstream.Msg) {
		n := inflight.Add(1)
		for {
			old := maxInflight.Load()
			if n <= old || maxInflight.CompareAndSwap(old, n) {
				break
			}
		}
		s := streamSeq(t, m)
		if s == 1 {
			close(first)
			<-release // a slow handler for message 1
		}
		mu.Lock()
		finished = append(finished, s)
		mu.Unlock()
		inflight.Add(-1)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Stop()
	if _, ok := recvWithin(first, 5*time.Second); !ok {
		t.Fatal("message 1 not delivered")
	}
	e.proxy.Cut() // reconnect -> errConnected -> reset
	harness.Eventually(5*time.Second, func() bool { return maxInflight.Load() > 1 })
	close(release)
	harness.Eventually(5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(finished) == 3
	})
	mu.Lock()
	defer mu.Unlock()
	if maxInflight.Load() > 1 {
		t.Errorf("handler ran for %d messages at once; handler completion order %v", maxInflight.Load(), finished)
	}
	for i, s := range finished {
		if s != uint64(i+1) {
			t.Errorf("handler completion order %v, want 1, 2, 3", finished)
			break
		}
	}
}

// ORD-5: Stop on a Messages iterator while Next is inside reset(). The CAS
// in Stop happens after reset() checked closed, so the reset succeeds. If
// Stop then gets the mutex before resetMessages installs the new inner
// subscription (ordered.go:348-354), it stops the old one, and the new one
// is never stopped: Next keeps returning messages after Stop and the inner
// subscription leaks. Which of the two takes the mutex first after reset()
// is up to the scheduler (the bad order shows up in roughly 1 of 20
// attempts here), so the test repeats the forced part of the trace.
// Trace: OrderedMessages_resurrect_bug.cfg.
func TestORD5MessagesNextAfterStopDuringReset(t *testing.T) {
	e := newEnv(t, envOpts{relay: true})
	for i := 0; i < 600; i++ {
		if seq, ok := ord5Attempt(t, e); !ok {
			t.Fatalf("attempt %d: Next after Stop returned stream seq %d, want ErrMsgIteratorClosed", i, seq)
		}
	}
}

func ord5Attempt(t *testing.T, e *env) (uint64, bool) {
	e.publish(1)
	c := e.ordered(jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: e.seq})
	it, err := c.Messages()
	if err != nil {
		t.Fatal(err)
	}
	defer it.Stop()
	// The first Next issues the inner pull request, which stays waiting on
	// the server after the message.
	if _, err := it.Next(jetstream.NextMaxWait(5 * time.Second)); err != nil {
		t.Fatalf("first Next: %v", err)
	}
	create := e.rl.HoldNext("CONSUMER.CREATE.")
	inflight := make(chan error, 1)
	go func() {
		_, err := it.Next(jetstream.NextMaxWait(5 * time.Second))
		inflight <- err
	}()
	e.deleteConsumer(consumerName(t, c)) // 409 -> Next -> resetMessages -> reset()
	h := waitHeld(t, create, "the consumer create of reset()")
	e.publish(2)
	stopped := make(chan struct{})
	go func() {
		it.Stop()
		close(stopped)
	}()
	waitBlockedIn(t, "(*orderedSubscription).Stop")
	h.Release()
	if _, ok := recvWithin(stopped, 10*time.Second); !ok {
		t.Fatal("Stop did not return")
	}
	if _, ok := recvWithin(inflight, 10*time.Second); !ok {
		t.Fatal("in-flight Next did not return")
	}
	m, err := it.Next(jetstream.NextMaxWait(500 * time.Millisecond))
	if err == nil {
		return streamSeq(t, m), false
	}
	if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
		t.Fatalf("Next after Stop: %v, want ErrMsgIteratorClosed", err)
	}
	return 0, true
}

// ORD-6: Stop on a Consume while the reset goroutine is inside reset(). As
// in ORD-5 the reset succeeds; after Stop has returned the goroutine
// creates a new inner Consume (ordered.go:183), which pulls from the new
// server-side consumer, and only then notices done (ordered.go:190-197).
// Messages that arrive in between reach the handler after Stop returned.
// The test asserts that no pull request is issued after Stop returned and
// logs late handler calls (their number depends on scheduling).
// Trace: StopResurrect_bug.cfg.
func TestORD6ConsumeSubscribesAfterStop(t *testing.T) {
	e := newEnv(t, envOpts{relay: true})
	e.publish(1)
	c := e.ordered(jetstream.OrderedConsumerConfig{})
	var stopAt atomic.Pointer[time.Time]
	var late atomic.Int32
	got := make(chan uint64, 16)
	cc, err := c.Consume(func(m jetstream.Msg) {
		if stopAt.Load() != nil {
			late.Add(1)
		}
		got <- streamSeq(t, m)
	})
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := recvWithin(got, 5*time.Second); !ok || s != 1 {
		t.Fatalf("first message: %d %v", s, ok)
	}
	e.publish(3)
	create := e.rl.HoldNext("CONSUMER.CREATE.")
	e.deleteConsumer(consumerName(t, c)) // 409 -> errHandler -> doReset -> reset()
	h := waitHeld(t, create, "the consumer create of reset()")
	stopped := make(chan struct{})
	go func() {
		cc.Stop()
		now := time.Now()
		stopAt.Store(&now)
		close(stopped)
	}()
	waitBlockedIn(t, "(*orderedSubscription).Stop")
	h.Release()
	if _, ok := recvWithin(stopped, 10*time.Second); !ok {
		t.Fatal("Stop did not return")
	}
	<-cc.Closed()
	time.Sleep(500 * time.Millisecond)
	var pulls []string
	for _, s := range e.rl.Since(*stopAt.Load()) {
		if strings.HasPrefix(s, "CONSUMER.MSG.NEXT.") {
			pulls = append(pulls, s)
		}
	}
	if late.Load() > 0 {
		t.Logf("%d handler call(s) after Stop returned", late.Load())
	}
	if len(pulls) > 0 {
		t.Fatalf("pull request(s) issued after Stop returned: %v", pulls)
	}
}

// ORD-7: the connection closes while reset() is retrying the consumer
// create. reset() already stopped the inner subscription, whose status
// goroutine was the only listener for CLOSED, so nothing stops the ordered
// subscription: reset() retries forever (MaxResetAttempts defaults to
// unlimited, every attempt fails with ErrConnectionClosed), Consume never
// reports ErrConnectionClosed or closes, and Messages' Next never returns.
// Trace: CloseDuringReset_bug.cfg, OrderedMessages_close_bug.cfg.
func TestORD7CloseDuringResetNeverEnds(t *testing.T) {
	t.Run("Consume", func(t *testing.T) {
		e := newEnv(t, envOpts{relay: true})
		c := e.ordered(jetstream.OrderedConsumerConfig{})
		closedErr := make(chan struct{})
		var once sync.Once
		cc, err := c.Consume(func(m jetstream.Msg) {}, jetstream.ConsumeErrHandler(
			func(_ jetstream.ConsumeContext, err error) {
				if errors.Is(err, jetstream.ErrConnectionClosed) {
					once.Do(func() { close(closedErr) })
				}
			}))
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
		create := e.rl.HoldNext("CONSUMER.CREATE.")
		e.deleteConsumer(consumerName(t, c))
		waitHeld(t, create, "the consumer create of reset()")
		e.nc.Close()
		if _, ok := recvWithin(cc.Closed(), 15*time.Second); !ok {
			t.Fatal("consume did not end after the connection closed during a reset")
		}
		if _, ok := recvWithin(closedErr, time.Second); !ok {
			t.Error("ErrConnectionClosed was not reported")
		}
	})
	t.Run("Messages", func(t *testing.T) {
		e := newEnv(t, envOpts{relay: true})
		c := e.ordered(jetstream.OrderedConsumerConfig{})
		it, err := c.Messages()
		if err != nil {
			t.Fatal(err)
		}
		next := make(chan error, 1)
		go func() {
			_, err := it.Next()
			next <- err
		}()
		time.Sleep(200 * time.Millisecond)
		create := e.rl.HoldNext("CONSUMER.CREATE.")
		e.deleteConsumer(consumerName(t, c))
		waitHeld(t, create, "the consumer create of reset()")
		e.nc.Close()
		err, ok := recvWithin(next, 15*time.Second)
		if !ok {
			t.Fatal("Next did not return after the connection closed during a reset")
		}
		if !errors.Is(err, jetstream.ErrMsgIteratorClosed) || !errors.Is(err, jetstream.ErrConnectionClosed) {
			t.Errorf("Next: %v, want ErrMsgIteratorClosed and ErrConnectionClosed", err)
		}
		if _, err := it.Next(jetstream.NextMaxWait(time.Second)); !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			t.Errorf("second Next: %v, want ErrMsgIteratorClosed", err)
		}
	})
}

// ORD-8: Next(NextMaxWait / NextContext) does not bound the time spent in
// resetMessages: reset() holds the ordered consumer's mutex across
// CreateOrUpdateConsumer and its backoff (ordered.go:583-595), ignoring the
// caller's deadline. Trace: OrderedMessages_timeout_bug.cfg. With the fix,
// the interrupted reset is left pending and the next Next completes it.
func TestORD8NextDeadlineIgnoredDuringReset(t *testing.T) {
	for _, kind := range []string{"NextMaxWait", "NextContext"} {
		t.Run(kind, func(t *testing.T) {
			e := newEnv(t, envOpts{relay: true})
			c := e.ordered(jetstream.OrderedConsumerConfig{})
			it, err := c.Messages()
			if err != nil {
				t.Fatal(err)
			}
			defer it.Stop()
			// Issue the inner pull request so that the deletion below is
			// answered with 409 Consumer Deleted.
			if _, err := it.Next(jetstream.NextMaxWait(200 * time.Millisecond)); !errors.Is(err, nats.ErrTimeout) {
				t.Fatalf("priming Next: %v", err)
			}
			creates, stopHolding := e.rl.HoldAll("CONSUMER.CREATE.")
			defer stopHolding()
			e.deleteConsumer(consumerName(t, c))
			start := time.Now()
			done := make(chan error, 1)
			go func() {
				var err error
				if kind == "NextMaxWait" {
					_, err = it.Next(jetstream.NextMaxWait(time.Second))
				} else {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					_, err = it.Next(jetstream.NextContext(ctx))
				}
				done <- err
			}()
			h := waitHeld(t, creates, "the consumer create of reset()")
			err, ok := recvWithin(done, 4*time.Second)
			stopHolding()
			h.Release()
			if !ok {
				err = <-done
				t.Fatalf("Next with a 1s deadline returned after %v (%v)", time.Since(start).Round(time.Millisecond), err)
			}
			want := nats.ErrTimeout
			if kind == "NextContext" {
				want = context.DeadlineExceeded
			}
			if !errors.Is(err, want) {
				t.Fatalf("Next: %v, want %v", err, want)
			}
			// The reset the deadline interrupted is retried by the next
			// call, which then delivers from where the iterator was.
			e.publish(1)
			m, err := it.Next(jetstream.NextMaxWait(10 * time.Second))
			if err != nil {
				t.Fatalf("Next after the interrupted reset: %v", err)
			}
			if s := streamSeq(t, m); s != 1 {
				t.Fatalf("Next after the interrupted reset: stream seq %d, want 1", s)
			}
		})
	}
}

// ORD-12: Messages never recovers when its server-side consumer disappears
// while no pull request is waiting (deleted, or removed by the server after
// InactiveThreshold because the application paused between Next calls).
// The next pull is answered with 503 No Responders, which the inner Next
// swallows (pull.go:697-702) without a reset, and the only other way out,
// the missed-heartbeat timer, is created per Next call (pull.go:659-664),
// so it never fires when Next is called with a deadline shorter than two
// heartbeats. Consume resets on ErrNoResponders (ordered.go:225); Messages
// does not. Trace: OrderedMessages_noresp_bug.cfg.
func TestORD12MessagesStuckAfterSilentConsumerLoss(t *testing.T) {
	harness.KnownBug(t, "ORD-12")
	poll := func(t *testing.T, it jetstream.MessagesContext, want uint64) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			m, err := it.Next(jetstream.NextMaxWait(500 * time.Millisecond))
			if err == nil {
				if s := streamSeq(t, m); s != want {
					t.Fatalf("got stream seq %d, want %d", s, want)
				}
				return
			}
			if !errors.Is(err, nats.ErrTimeout) {
				t.Fatalf("Next: %v", err)
			}
		}
		t.Fatalf("no message within 15s of polling Next(NextMaxWait(500ms)); stream has seq %d", want)
	}
	t.Run("deleted", func(t *testing.T) {
		e := newEnv(t, envOpts{})
		c := e.ordered(jetstream.OrderedConsumerConfig{})
		it, err := c.Messages()
		if err != nil {
			t.Fatal(err)
		}
		defer it.Stop()
		e.deleteConsumer(consumerName(t, c)) // no pull waiting yet
		e.publish(1)
		poll(t, it, 1)
	})
	t.Run("inactive", func(t *testing.T) {
		e := newEnv(t, envOpts{})
		c := e.ordered(jetstream.OrderedConsumerConfig{InactiveThreshold: time.Second})
		it, err := c.Messages(jetstream.PullExpiry(2 * time.Second))
		if err != nil {
			t.Fatal(err)
		}
		defer it.Stop()
		e.publish(1)
		poll(t, it, 1)
		// The application pauses: the pull request expires and the server
		// removes the idle consumer.
		name := consumerName(t, c)
		if !harness.Eventually(15*time.Second, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := e.ajs.Consumer(ctx, streamName, name)
			return errors.Is(err, jetstream.ErrConsumerNotFound)
		}) {
			t.Fatal("server did not remove the inactive consumer")
		}
		e.publish(1)
		poll(t, it, 2)
	})
}

// ORD-13: the ordered consumer's default idle heartbeat (5s, chosen by
// setDefaults(ordered=true), pull.go:1245-1251) never reaches the inner
// pull subscription, which re-parses the options with ordered=false
// (pull.go:206, 512) and uses Expires/2 = 15s. A stalled pull is detected
// after 30s instead of 10s. The test stalls every pull request in the relay
// and measures how long Consume takes to start a reset.
func TestORD13OrderedDefaultHeartbeatNotApplied(t *testing.T) {
	e := newEnv(t, envOpts{relay: true})
	c := e.ordered(jetstream.OrderedConsumerConfig{})
	_, stopHolding := e.rl.HoldAll("CONSUMER.MSG.NEXT.")
	defer stopHolding()
	resets := e.rl.HoldNext("CONSUMER.CREATE.")
	start := time.Now()
	cc, err := c.Consume(func(m jetstream.Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Stop()
	h, ok := recvWithin(resets, 15*time.Second)
	if !ok {
		t.Fatalf("every pull is stalled but no reset within 15s (ordered default heartbeat is 5s: reset expected after 10s)")
	}
	t.Logf("reset after %v", time.Since(start).Round(time.Millisecond))
	h.Release()
}
