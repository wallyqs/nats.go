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
)

// The legacy JetStream API (js.go) shares the async publisher design. These
// tests check the model's findings against it where the code is the same,
// and the one place it is weaker (max pending).

func legacyJS(t *testing.T, e *env, opts ...nats.JSOpt) nats.JetStreamContext {
	t.Helper()
	js, err := e.nc.JetStream(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

// PUB-1 in js.go: the ack-timeout callback (js.go PublishMsgAsync,
// time.AfterFunc(js.opts.ackTimeout, ...)) also called aecb under js.mu.
func TestPUB1LegacyTimeoutErrHandlerReentry(t *testing.T) {
	e := newEnv(t, nil)
	entered := make(chan struct{}, 1)
	returned := make(chan struct{}, 1)
	js := legacyJS(t, e,
		nats.PublishAsyncTimeout(50*time.Millisecond),
		nats.PublishAsyncErrHandler(func(js nats.JetStream, m *nats.Msg, err error) {
			entered <- struct{}{}
			if js.PublishAsyncPending() > 100 {
				return
			}
			returned <- struct{}{}
		}))
	if _, err := js.PublishAsync("silent.a", []byte("x")); err != nil {
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
		t.Fatal("PUB-1 (legacy): MsgErrHandler deadlocked calling PublishAsyncPending from the ack-timeout callback")
	}
}

// PUB-L1: js.go registered the future before checking the cap (registerPAF,
// then `numPending > maxPending` stall), so PublishAsyncPending exceeded
// PublishAsyncMaxPending while a caller was stalled; and it waited on the
// stall channel once, without re-checking the cap. #2126 fixed both in
// jetstream/; js.go now has the same registerPAF (stall, then draw the
// token and insert under one lock hold). Model: MaxPendingRespected holds
// for jetstream/.
func TestPUBL1LegacyMaxPendingExceeded(t *testing.T) {
	e := newEnv(t, nil)
	js := legacyJS(t, e, nats.PublishAsyncMaxPending(1))
	if _, err := js.PublishAsync("silent.a", nil); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := js.PublishAsync("silent.b", nil, nats.StallWait(time.Second))
		done <- err
	}()
	max := 0
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := js.PublishAsyncPending(); n > max {
			max = n
		}
		time.Sleep(time.Millisecond)
	}
	if err := <-done; !errors.Is(err, nats.ErrTooManyStalledMsgs) {
		t.Logf("stalled publish returned %v", err)
	}
	if max > 1 {
		t.Fatalf("PUB-L1: PublishAsyncPending reached %d with PublishAsyncMaxPending(1)", max)
	}
}

// PUB-2 in js.go: CleanupPublisher did not close js.stc either. With the
// PUB-L1 fix the stalled caller registers only once woken, right after
// CleanupPublisher removed the reply subscription -- the PUB-6 window -- so
// the test also checks that its future is acked.
func TestPUB2LegacyCleanupDoesNotWakeStalledPublisher(t *testing.T) {
	e := newEnv(t, nil)
	js := legacyJS(t, e, nats.PublishAsyncMaxPending(1))
	if _, err := js.PublishAsync("silent.a", nil); err != nil {
		t.Fatal(err)
	}
	type result struct {
		paf nats.PubAckFuture
		err error
	}
	done := make(chan result, 1)
	go func() {
		paf, err := js.PublishAsync("s.b", nil, nats.StallWait(3*time.Second))
		done <- result{paf, err}
	}()
	waitGoroutine(t, "the max-pending stall", "[select", "(*js).registerPAF")
	js.CleanupPublisher()
	var r result
	select {
	case r = <-done:
		if r.err != nil {
			t.Fatalf("PUB-2 (legacy): stalled publish failed with %v although CleanupPublisher emptied the publisher", r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("PUB-2 (legacy): stalled publish still parked 1s after CleanupPublisher")
	}
	select {
	case <-r.paf.Ok():
	case err := <-r.paf.Err():
		t.Fatalf("publish woken by CleanupPublisher failed: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatalf("PUB-6 (legacy): publish woken by CleanupPublisher was registered under the removed reply subscription and never resolved (pending=%d)", js.PublishAsyncPending())
	}
	if n := js.PublishAsyncPending(); n != 0 {
		t.Fatalf("pending = %d", n)
	}
}
