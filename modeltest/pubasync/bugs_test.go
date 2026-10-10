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
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/modeltest/harness"
)

// Each test below reproduces one counterexample of models/PublishAsync
// (models/PublishAsync/PUB*_bug.cfg) against the real library and asserts
// the correct behavior. Tests for fixed findings are regression tests; those
// still gated by harness.KnownBug fail on this code and are skipped unless
// MODELTEST_KNOWN_BUGS=1.

// PUB-1: the ack-timeout callback (publish.go:373-405) ran the
// MsgErrHandler while holding js.publisher's mutex. A handler that calls back
// into the publisher -- PublishAsyncPending, or re-publishing the message as
// MsgErrHandler's doc suggests ("for possible retransmitting") -- deadlocked
// the timer goroutine, and with it every later publisher call. Fixed: the
// handler now runs after Unlock, like on every other resolution path.
// Model: PUB1_timeout_reentrant_bug.cfg (NoSelfDeadlock).
func TestPUB1TimeoutErrHandlerReentry(t *testing.T) {
	entered := make(chan struct{}, 1)
	returned := make(chan struct{}, 1)
	e := newEnv(t, []jetstream.JetStreamOpt{
		jetstream.WithPublishAsyncTimeout(50 * time.Millisecond),
		jetstream.WithPublishAsyncErrHandler(func(js jetstream.JetStream, m *nats.Msg, err error) {
			entered <- struct{}{}
			if js.PublishAsyncPending() > 100 { // e.g. decide whether to retransmit
				return
			}
			returned <- struct{}{}
		}),
	})
	if _, err := e.js.PublishAsync("silent.a", []byte("x")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("ack timeout never fired")
	}
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("PUB-1: MsgErrHandler deadlocked calling PublishAsyncPending from the ack-timeout callback (publisher mutex held, publish.go:396-398)")
	}
}

// PUB-2: CleanupPublisher (jetstream.go:1295-1315) deletes every pending
// future but never calls maybeUnstall, so a PublishMsgAsync parked in
// registerPAF's stall (publish.go:626-640) is not woken although the
// publisher is now empty; it sleeps out its whole stall wait and fails with
// ErrTooManyStalledMsgs. Model: PUB2_cleanup_stall_bug.cfg (NoLostStallWakeup).
func TestPUB2CleanupDoesNotWakeStalledPublisher(t *testing.T) {
	harness.KnownBug(t, "PUB-2")
	e := newEnv(t, []jetstream.JetStreamOpt{jetstream.WithPublishAsyncMaxPending(1)})
	if _, err := e.js.PublishAsync("silent.a", nil); err != nil { // holds the only slot
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := e.js.PublishAsync("s.b", nil, jetstream.WithStallWait(3*time.Second))
		done <- err
	}()
	waitGoroutine(t, "the max-pending stall", "[select", "registerPAF")
	e.js.CleanupPublisher()
	if n := e.js.PublishAsyncPending(); n != 0 {
		t.Fatalf("pending after cleanup = %d", n)
	}
	start := time.Now()
	select {
	case err := <-done:
		if errors.Is(err, jetstream.ErrTooManyStalledMsgs) {
			t.Fatalf("PUB-2: stalled publish failed with %v after %v although CleanupPublisher emptied the publisher", err, time.Since(start))
		}
	case <-time.After(time.Second):
		t.Fatalf("PUB-2: stalled publish still parked 1s after CleanupPublisher emptied the publisher (pending=%d)", e.js.PublishAsyncPending())
	}
}

// PUB-3 (retry path): a 503 retry whose PublishMsg fails synchronously went
// through clearPAF + handleAsyncErrAndUnlock (publish.go:511-518, 683-688);
// neither closed js.publisher.doneCh, so a PublishAsyncComplete channel taken
// while that future was the last one pending was never closed.
// Model: PUB3_retry_complete_bug.cfg (NoLostCompletion).
//
// The model's trace fails the retry with nc.Close(); since PUB-5 the reconnect
// worker fails the future on CLOSED first, so here the retry's PublishMsg is
// made to fail while reconnecting without a reconnect buffer instead: the
// reconnect dial is parked holding nc.mu (the retry blocks on it), and the
// reconnect worker is kept busy in a blocked MsgErrHandler from an earlier
// disconnect so that it does not fail the future before the retry does.
func TestPUB3RetryFailureLeavesCompleteOpen(t *testing.T) {
	h := newHandlerLog()
	e := newEnv(t, []jetstream.JetStreamOpt{jetstream.WithPublishAsyncErrHandler(h.handler)},
		nats.ReconnectBufSize(-1), nats.ReconnectWait(50*time.Millisecond))
	e.warmUp()
	busy, releaseHandler := h.blockOn("silent.block")
	defer releaseHandler()
	if _, err := e.js.PublishAsync("silent.block", nil); err != nil {
		t.Fatal(err)
	}
	e.cutAndAwaitReconnect()
	<-busy

	paf, err := e.js.PublishAsync("nostream.a", []byte("x"),
		jetstream.WithRetryAttempts(1), jetstream.WithRetryWait(300*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // the 503 arrives; the retry is scheduled 300ms out
	complete := e.js.PublishAsyncComplete()
	if closedWithin(complete, 0) {
		t.Fatal("complete closed while a future is pending")
	}
	dialing, releaseDial := e.dial.arm()
	defer releaseDial()
	e.proxy.Cut()
	<-dialing
	waitGoroutine(t, "the retry's PublishMsg", "PublishMsgAsync", "(*Conn).publish", "sync.(*Mutex).Lock")
	releaseDial() // the dial fails; the retry's PublishMsg returns ErrReconnectBufExceeded
	select {
	case err := <-paf.Err():
		if !errors.Is(err, nats.ErrReconnectBufExceeded) {
			t.Fatalf("unexpected future error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retry never failed the future")
	}
	if n := e.js.PublishAsyncPending(); n != 0 {
		t.Fatalf("pending = %d", n)
	}
	if !closedWithin(complete, time.Second) {
		t.Fatal("PUB-3: PublishAsyncComplete never closed although nothing is pending (retry failed synchronously)")
	}
}

// PUB-3 (first attempt): PublishMsg failing after registerPAF has inserted
// the future (publish.go:422-425) cleared it without closing doneCh. The
// dial gate parks the reconnect inside createConn holding nc.mu, so the
// publish blocks between registerPAF and the failure; PublishAsyncComplete
// taken in that window never closed. Fixed: clearPAF hands doneCh back to
// be closed once the outcome is reported.
// Model: PUB3_first_publish_complete_bug.cfg (NoLostCompletion).
func TestPUB3FirstPublishFailureLeavesCompleteOpen(t *testing.T) {
	e := newEnv(t, nil, nats.ReconnectBufSize(-1), nats.ReconnectWait(300*time.Millisecond))
	e.warmUp()
	entered, release := e.dial.arm()
	defer release()
	e.proxy.Cut()
	<-entered // reconnect goroutine holds nc.mu inside the dial
	// The publisher's reconnect worker has handled RECONNECTING (nothing
	// was pending) and is back on its channel.
	waitGoroutine(t, "the idle reconnect worker", "resetPendingAcksOnReconnect", "[chan receive")

	pubErr := make(chan error, 1)
	go func() {
		_, err := e.js.PublishAsync("s.a", []byte("x"))
		pubErr <- err
	}()
	if !harness.Eventually(5*time.Second, func() bool { return e.js.PublishAsyncPending() == 1 }) {
		t.Fatal("publish never registered")
	}
	complete := e.js.PublishAsyncComplete()
	release() // dial fails; the publish now sees RECONNECTING with no buffer
	select {
	case err := <-pubErr:
		if !errors.Is(err, nats.ErrReconnectBufExceeded) {
			t.Fatalf("expected ErrReconnectBufExceeded, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("publish never returned")
	}
	if n := e.js.PublishAsyncPending(); n != 0 {
		t.Fatalf("pending = %d", n)
	}
	if !closedWithin(complete, time.Second) {
		t.Fatal("PUB-3: PublishAsyncComplete never closed although nothing is pending (first publish failed synchronously)")
	}
}

// pub4Setup drives the PUB-4 interleaving: the 503-retry goroutine passes
// getPAF (publish.go:505-510) for future X, the reconnect worker then fails X
// with ErrDisconnected (publish.go:585-594), and only then does the retry's
// PublishMsg fail, after which handleAsyncErrAndUnlock resolved X a second
// time (publish.go:515-518). Fixed: the PublishMsg failure branch resolves
// the future only if it is still pending. To pin that order: the reconnect
// worker is kept busy in a blocked MsgErrHandler from an earlier disconnect
// (so the second RECONNECTING event waits in its channel), and the reconnect
// dial is parked holding nc.mu (so the retry's PublishMsg waits).
func pub4Setup(t *testing.T, callErr bool) (*env, *handlerLog, jetstream.PubAckFuture, <-chan error, func()) {
	h := newHandlerLog()
	e := newEnv(t, []jetstream.JetStreamOpt{jetstream.WithPublishAsyncErrHandler(h.handler)},
		nats.ReconnectBufSize(-1), nats.ReconnectWait(50*time.Millisecond))
	e.warmUp()

	// 1. Park the reconnect worker inside the error handler.
	busy, releaseHandler := h.blockOn("silent.block")
	if _, err := e.js.PublishAsync("silent.block", nil); err != nil {
		t.Fatal(err)
	}
	e.cutAndAwaitReconnect()
	<-busy

	// 2. Future X gets a 503 and schedules its retry.
	x, err := e.js.PublishAsync("nostream.x", []byte("x"),
		jetstream.WithRetryAttempts(1), jetstream.WithRetryWait(300*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	var errCh <-chan error
	if callErr {
		errCh = x.Err() // a caller holding Err() but not reading it yet
	}
	time.Sleep(100 * time.Millisecond) // 503 processed, retry timer armed

	// 3. Disconnect again; the RECONNECTING event waits in the channel and
	//    the reconnect dial parks while holding nc.mu.
	dialing, releaseDial := e.dial.arm()
	e.proxy.Cut()
	<-dialing

	// 4. The retry fires, passes getPAF and blocks in nc.publish on nc.mu.
	waitGoroutine(t, "the retry's PublishMsg", "PublishMsgAsync", "(*Conn).publish", "sync.(*Mutex).Lock")

	// 5. Let the reconnect worker run: it fails X with ErrDisconnected.
	releaseHandler()
	if !harness.Eventually(5*time.Second, func() bool { return len(h.get("nostream.x")) >= 1 }) {
		t.Fatal("reconnect worker never reported X")
	}
	if n := e.js.PublishAsyncPending(); n != 0 {
		t.Fatalf("pending = %d", n)
	}
	// 6. Let the dial fail: the retry's PublishMsg returns ErrReconnectBufExceeded.
	releaseDial()
	time.Sleep(200 * time.Millisecond)
	return e, h, x, errCh, releaseDial
}

// PUB-4a: the same message was reported to MsgErrHandler twice (once with
// ErrDisconnected, once with ErrReconnectBufExceeded) and the future's error
// overwritten. Model: PUB4_retry_double_cb_bug.cfg (ErrCbAtMostOnce).
func TestPUB4RetryErrorAfterReconnectReportsTwice(t *testing.T) {
	_, h, _, _, _ := pub4Setup(t, false)
	time.Sleep(500 * time.Millisecond)
	if got := h.get("nostream.x"); len(got) != 1 {
		t.Fatalf("PUB-4: MsgErrHandler called %d times for one message: %v", len(got), got)
	}
}

// PUB-4b: if the caller already holds paf.Err() (cap 1) and has not read
// the first error, the retry goroutine's second send blocked forever while
// holding js.publisher's mutex: every later PublishAsync,
// PublishAsyncPending, Ok()/Err() call and the ack handler hung.
// Model: PUB4_retry_double_resolve_bug.cfg (NoBlockedSendUnderLock).
func TestPUB4RetryErrorAfterReconnectWedgesPublisher(t *testing.T) {
	e, _, _, errCh, _ := pub4Setup(t, true)
	var n int
	if !within(2*time.Second, func() { n = e.js.PublishAsyncPending() }) {
		t.Fatal("PUB-4: publisher wedged: the 503-retry goroutine is blocked sending a second error into paf.errCh while holding js.publisher's mutex")
	}
	if n != 0 {
		t.Fatalf("pending = %d", n)
	}
	first := <-errCh
	select {
	case second := <-errCh:
		t.Fatalf("PUB-4: future resolved twice: %v then %v", first, second)
	case <-time.After(300 * time.Millisecond):
	}
}

// PUB-5: resetPendingAcksOnReconnect returns on CLOSED without failing the
// pending futures (publish.go:578-581), and nothing else ever will: their
// Ok()/Err() never fire and PublishAsyncComplete never closes unless an ack
// timeout was configured. Model: PUB5_closed_never_resolved_bug.cfg
// (NoStrandedFuture).
func TestPUB5CloseStrandsPendingFutures(t *testing.T) {
	harness.KnownBug(t, "PUB-5")
	e := newEnv(t, nil)
	paf, err := e.js.PublishAsync("silent.a", []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	complete := e.js.PublishAsyncComplete()
	e.nc.Close()
	select {
	case err := <-paf.Err():
		t.Logf("future failed with %v", err)
	case <-paf.Ok():
		t.Fatal("acked?")
	case <-time.After(2 * time.Second):
		t.Fatalf("PUB-5: future pending at nc.Close() never resolved (pending=%d)", e.js.PublishAsyncPending())
	}
	if !closedWithin(complete, time.Second) {
		t.Fatal("PUB-5: PublishAsyncComplete never closed after nc.Close()")
	}
}

// PUB-7a: nc.sendStatusEvent probes "is the listener closed?" with a
// receive (nats.go sendStatusEvent), which also succeeds on a buffered,
// not-yet-consumed event: it eats that event and unregisters the listener.
// While the publisher's reconnect worker is busy in a MsgErrHandler, two
// more disconnects therefore unsubscribe it from RECONNECTING for good, and
// later reconnects no longer fail pending futures.
// Model: PUB7_status_probe_deaf_bug.cfg (ListenerSubscribed).
func TestPUB7PublisherGoesDeafToReconnects(t *testing.T) {
	harness.KnownBug(t, "PUB-7")
	h := newHandlerLog()
	e := newEnv(t, []jetstream.JetStreamOpt{jetstream.WithPublishAsyncErrHandler(h.handler)})
	e.warmUp()
	busy, releaseHandler := h.blockOn("silent.block")
	if _, err := e.js.PublishAsync("silent.block", nil); err != nil {
		t.Fatal(err)
	}
	e.cutAndAwaitReconnect() // RECONNECTING #1: worker enters the handler and blocks
	<-busy
	e.cutAndAwaitReconnect() // #2 is buffered
	e.cutAndAwaitReconnect() // #3: the probe eats #2 and unregisters the listener
	releaseHandler()
	time.Sleep(100 * time.Millisecond)

	paf, err := e.js.PublishAsync("silent.later", nil)
	if err != nil {
		t.Fatal(err)
	}
	e.cutAndAwaitReconnect() // #4
	select {
	case err := <-paf.Err():
		if !errors.Is(err, nats.ErrDisconnected) {
			t.Fatalf("unexpected error %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("PUB-7: future pending across a reconnect was not failed with ErrDisconnected: the publisher's status listener was dropped (pending=%d)", e.js.PublishAsyncPending())
	}
}

// PUB-7b: RemoveStatusListener closes the channel only if a receive finds
// it empty (select { case <-ch: default: close(ch) }). CleanupPublisher
// while a RECONNECTING event is buffered therefore eats the event, leaves
// the channel open and unregistered, and the reconnect worker goroutine
// blocks on it forever. Model: PUB7_remove_listener_leak_bug.cfg
// (NoLeakedResetGoroutine).
func TestPUB7CleanupLeaksReconnectWorker(t *testing.T) {
	harness.KnownBug(t, "PUB-7")
	h := newHandlerLog()
	e := newEnv(t, []jetstream.JetStreamOpt{jetstream.WithPublishAsyncErrHandler(h.handler)})
	worker := "resetPendingAcksOnReconnect"
	// Workers of earlier tests exit once their connection's CLOSED event
	// is handled; let them drain before taking the baseline.
	harness.Eventually(2*time.Second, func() bool { return countGoroutines(worker) == 0 })
	base := countGoroutines(worker)
	e.warmUp()
	busy, releaseHandler := h.blockOn("silent.block")
	if _, err := e.js.PublishAsync("silent.block", nil); err != nil {
		t.Fatal(err)
	}
	e.cutAndAwaitReconnect()
	<-busy
	e.cutAndAwaitReconnect() // a RECONNECTING event waits in the channel
	e.js.CleanupPublisher()  // RemoveStatusListener eats it instead of closing
	releaseHandler()
	if !harness.Eventually(2*time.Second, func() bool { return countGoroutines(worker) <= base }) {
		t.Fatalf("PUB-7: reconnect worker leaked after CleanupPublisher (%d running, %d before)", countGoroutines(worker), base)
	}
}

// PUB-8: a future registered by PublishMsgAsync is failed (and reported to
// MsgErrHandler) by the ack timer while the publish itself is still blocked
// in nc.PublishMsg; when that then failed, PublishMsgAsync also returned the
// error. The same message was reported twice, through two channels, so a
// caller that retransmits on both sent it twice. Fixed: PublishMsgAsync then
// returns the (already failed) future and no error.
// Model: PUB8_double_report_bug.cfg (SingleReport).
func TestPUB8SyncErrorAfterHandlerReport(t *testing.T) {
	h := newHandlerLog()
	e := newEnv(t, []jetstream.JetStreamOpt{
		jetstream.WithPublishAsyncErrHandler(h.handler),
		jetstream.WithPublishAsyncTimeout(100 * time.Millisecond),
	}, nats.ReconnectBufSize(-1), nats.ReconnectWait(300*time.Millisecond))
	e.warmUp()
	entered, release := e.dial.arm()
	defer release()
	e.proxy.Cut()
	<-entered
	waitGoroutine(t, "the idle reconnect worker", "resetPendingAcksOnReconnect", "[chan receive")
	pubErr := make(chan error, 1)
	go func() {
		_, err := e.js.PublishAsync("s.a", []byte("x"))
		pubErr <- err
	}()
	if !harness.Eventually(5*time.Second, func() bool { return len(h.get("s.a")) == 1 }) {
		t.Fatal("ack timer never reported the message")
	}
	release()
	err := <-pubErr
	if err != nil {
		t.Fatalf("PUB-8: message reported twice: MsgErrHandler got %v and PublishAsync returned %v", h.get("s.a"), err)
	}
}
