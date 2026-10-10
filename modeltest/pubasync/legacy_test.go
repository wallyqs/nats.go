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
	"github.com/nats-io/nats.go/modeltest/harness"
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

// PUB-L1: js.go registers the future before checking the cap (registerPAF,
// then `numPending > maxPending` stall), so PublishAsyncPending exceeds
// PublishAsyncMaxPending while a caller is stalled; and it waits on the
// stall channel once, without re-checking the cap. #2126 fixed both in
// jetstream/ only. Model: MaxPendingRespected holds for jetstream/.
func TestPUBL1LegacyMaxPendingExceeded(t *testing.T) {
	harness.KnownBug(t, "PUB-L1")
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

// PUB-2 in js.go: CleanupPublisher does not close js.stc either.
func TestPUB2LegacyCleanupDoesNotWakeStalledPublisher(t *testing.T) {
	harness.KnownBug(t, "PUB-2")
	e := newEnv(t, nil)
	js := legacyJS(t, e, nats.PublishAsyncMaxPending(1))
	if _, err := js.PublishAsync("silent.a", nil); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := js.PublishAsync("s.b", nil, nats.StallWait(3*time.Second))
		done <- err
	}()
	waitGoroutine(t, "the max-pending stall", "[select", "(*js).PublishMsgAsync")
	js.CleanupPublisher()
	select {
	case err := <-done:
		if errors.Is(err, nats.ErrTooManyStalledMsgs) {
			t.Fatalf("PUB-2 (legacy): stalled publish failed with %v although CleanupPublisher emptied the publisher", err)
		}
	case <-time.After(time.Second):
		t.Fatal("PUB-2 (legacy): stalled publish still parked 1s after CleanupPublisher")
	}
}
