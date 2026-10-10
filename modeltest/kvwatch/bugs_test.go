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

package kvwatch

// Regression tests for the model's counterexamples that need no test hooks,
// and the KV-9 reproducer (a nats-server bug, still gated).

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/modeltest/harness"
)

func stackContains(fn string) bool {
	buf := make([]byte, 4<<20)
	n := runtime.Stack(buf, true)
	return strings.Contains(string(buf[:n]), fn)
}

// KV-2: an UpdatesOnly watcher that has not received an update yet when
// the server restarts. The R1 memory ordered consumer is gone; after
// 2 x 5s without heartbeats activityCheck resets it from jsi.sseq+1, which
// was 1, and the watcher replayed every stored value as if it were new.
// Fixed: jsi.sseq starts at the consumer's Delivered.Stream (js.go).
// Model: KV2_reset_from_one_bug.cfg (NoStaleEntry).
func TestKV2UpdatesOnlyReplaysHistoryAfterServerRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the 10s heartbeat check")
	}
	e := newEnv(t, 1)
	e.put(t, "a", "a1")
	e.put(t, "b", "b1")
	w, err := e.kv.WatchAll(context.Background(), jetstream.UpdatesOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	e.srv.Restart()
	// Wait for activityCheck to recreate the consumer (one round trip
	// through the new consumer proves it is there).
	time.Sleep(12 * time.Second)
	e.put(t, "c", "c1")
	got, _ := collect(w, 10, 3*time.Second)
	if want := "[c@3=c1]"; fmtRecs(got) != want {
		t.Fatalf("UpdatesOnly watcher delivered %s after a server restart, want %s", fmtRecs(got), want)
	}
}

// KV-3: the watcher counts initial values against NumPending taken at
// consumer creation and otherwise waits for a message with delta 0. If the
// initial values it has not received yet are removed without a new
// message (MaxAge, PurgeDeletes(), a stream purge), no message ever
// arrives and the marker was never sent: Watch users never saw "initial
// values done" and Keys()/History() hung until their context ended.
// Fixed: an idle watcher asks ConsumerInfo whether anything is left
// (kv.go checkInit). The server is held mid-delivery by flow control while
// the watcher's reader is slow (the callback blocks on the full
// w.updates). Model: KV3_marker_stuck_bug.cfg (NoStuckInit).
func TestKV3MarkerNeverSentWhenPendingInitialValuesAreRemoved(t *testing.T) {
	e := newEnv(t, 1)
	ctx := context.Background()
	for i := 0; i < 260; i++ {
		e.put(t, keyN(i), "v")
	}
	big := strings.Repeat("x", 256*1024)
	for i := 0; i < 20; i++ {
		e.put(t, "L"+keyN(i), big)
	}
	w, err := e.kv.WatchAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	// Do not read yet: the callback blocks on w.updates after 256 entries,
	// flow control is not answered and the server stalls with some of the
	// large values still undelivered.
	st, err := e.js.Stream(ctx, "KV_"+e.bucket)
	if err != nil {
		t.Fatal(err)
	}
	var stalled bool
	harness.Eventually(5*time.Second, func() bool {
		cl := st.ListConsumers(ctx)
		for ci := range cl.Info() {
			stalled = ci.NumPending > 0 && ci.Delivered.Consumer > 256
		}
		return stalled
	})
	if !stalled {
		t.Fatal("server did not stall on flow control")
	}
	time.Sleep(200 * time.Millisecond)
	// Remove what the server has not sent yet (no new message is written).
	for i := 0; i < 20; i++ {
		if err := st.Purge(ctx, jetstream.WithPurgeSubject(e.subject("L"+keyN(i)))); err != nil {
			t.Fatal(err)
		}
	}
	got, ok := collectUntilMarker(w, 5*time.Second)
	if !ok {
		t.Fatalf("no init marker after %d entries; the watcher waits for values that no longer exist", len(got))
	}
}

// KV-5: KeyLister.Stop() is documented for stopping before all keys were
// read, but the lister goroutine sent on kl.keys without a select: once
// the caller stopped reading it blocked forever, Keys() was never closed,
// and the watcher callback stayed blocked on w.updates. Fixed: the lister
// also selects on Stop, and the callback gives up a send once the watcher
// is stopped (KV-6). Model: KV5_listkeys_leak_bug.cfg (NoLeakAfterStop).
func TestKV5ListKeysStopLeaksGoroutines(t *testing.T) {
	e := newEnv(t, 1)
	for i := 0; i < 1000; i++ {
		e.put(t, keyN(i), "v")
	}
	kl, err := e.kv.ListKeys(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-kl.Keys() // read one key, then give up
	time.Sleep(200 * time.Millisecond)
	if err := kl.Stop(); err != nil {
		t.Fatal(err)
	}
	// The lister goroutine and the watcher's update callback.
	for _, fn := range []string{"jetstream.newKeyLister.func1", "jetstream.(*kvs).WatchFiltered.func1"} {
		if !harness.Eventually(3*time.Second, func() bool { return !stackContains(fn) }) {
			t.Fatalf("%s still running 3s after KeyLister.Stop()", fn)
		}
	}
	// Keys() is closed.
	for range kl.Keys() {
	}
}

// KV-9 (nats-server, not nats.go): on a file-backed stream, a purge by
// subject followed by purges by sequence leaves the stream's state
// counters wrong (here FirstSeq 6 > LastSeq 5 with Msgs 0 while seq 5 is
// still stored and Get returns it). Consumers are created with that
// NumPending, so the watcher sends its marker early or delivers nothing:
// WatchAll misses b, History(b) says ErrKeyNotFound, Keys() says no keys.
// Found by TestKVWatchModel (removeKey + expire rules); MemoryStorage is
// not affected. Pinned against the embedded nats-server in go.mod.
func TestKV9FileStorePurgeAccountingHidesValues(t *testing.T) {
	harness.KnownBug(t, "KV-9")
	e := newEnv(t, 1)
	ctx := context.Background()
	st, err := e.js.Stream(ctx, "KV_"+e.bucket)
	if err != nil {
		t.Fatal(err)
	}
	e.put(t, "a", "a1") // 1
	e.put(t, "c", "c2") // 2
	if err := st.Purge(ctx, jetstream.WithPurgeSubject(e.subject("c"))); err != nil {
		t.Fatal(err)
	}
	if err := st.Purge(ctx, jetstream.WithPurgeSequence(2)); err != nil {
		t.Fatal(err)
	}
	e.put(t, "a", "a3") // 3
	e.put(t, "b", "b4") // 4 (replaced by 5, History=1)
	e.put(t, "b", "b5") // 5
	if err := st.Purge(ctx, jetstream.WithPurgeSequence(4)); err != nil {
		t.Fatal(err)
	}
	if g, err := e.kv.Get(ctx, "b"); err != nil || g.Revision() != 5 {
		t.Fatalf("Get(b) = %v, %v", g, err)
	}
	w, err := e.kv.WatchAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	got, _ := collectUntilMarker(w, 2*time.Second)
	if want := "[b@5=b5 <nil>]"; fmtRecs(got) != want {
		info, _ := st.Info(ctx)
		t.Fatalf("WatchAll delivered %s, want %s (stream state %+v)", fmtRecs(got), want, info.State)
	}
}
