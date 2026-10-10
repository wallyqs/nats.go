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

//go:build internal_testing

package kvwatch

// Deterministic reproducers that need the nats.go test hooks
// (testing_internal.go): AddMsgFilter drops or holds a delivered message
// exactly where the TLA+ counterexample does.

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/modeltest/harness"
)

// dropFirst makes the client lose the first JetStream message delivered
// on subject (the model's NetDrop): the ordered consumer then sees a gap.
func dropFirst(nc *nats.Conn, subject string) *atomic.Int32 {
	var n atomic.Int32
	nc.AddMsgFilter(subject, func(m *nats.Msg) *nats.Msg {
		if m.Reply != "" && n.Add(1) == 1 {
			return nil
		}
		return m
	})
	return &n
}

// KV-1: a gap while a DeliverLastPerSubject watcher is still delivering its
// initial values makes the ordered consumer restart with
// DeliverByStartSequence(jsi.sseq+1) (js.go:2274, 2380-2382), which
// replays every revision after the last delivered one: superseded
// revisions are handed out as initial values and the marker is counted
// against them, arriving before the real latest value. Model:
// KV1_stale_reset_bug.cfg (NoStaleEntry).
func TestKV1ResetDuringInitialPhaseDeliversStaleRevision(t *testing.T) {
	harness.KnownBug(t, "KV-1")
	e := newEnv(t, 5)
	e.put(t, "a", "a1")       // rev 1
	e.put(t, "b", "b1")       // rev 2 (superseded)
	e.put(t, "c", "c1")       // rev 3
	b2 := e.put(t, "b", "b2") // rev 4
	latest := map[string]uint64{"a": 1, "b": b2, "c": 3}

	drops := dropFirst(e.nc, e.subject("c"))
	w, err := e.kv.WatchAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	got, ok := collectUntilMarker(w, 5*time.Second)
	if !ok {
		t.Fatalf("no init marker; got %s", fmtRecs(got))
	}
	if drops.Load() == 0 {
		t.Fatal("fault not injected")
	}
	seen := map[string]bool{}
	for _, r := range got[:len(got)-1] {
		if r.rev < latest[r.key] {
			t.Errorf("stale revision %s delivered as an initial value (latest at watch start is %d); got %s",
				r, latest[r.key], fmtRecs(got))
		}
		seen[r.key] = r.rev >= latest[r.key]
	}
	for k := range latest {
		if !seen[k] {
			t.Errorf("marker before the latest value of %q was delivered; got %s", k, fmtRecs(got))
		}
	}
}

// KV-1, Keys flavor: the replay hands Keys() (IgnoreDeletes) the old Put of
// a key whose latest revision is a delete. Model: KV1_keys_deleted_bug.cfg
// (KeysSound).
func TestKV1KeysReturnsDeletedKeyAfterReset(t *testing.T) {
	harness.KnownBug(t, "KV-1")
	e := newEnv(t, 5)
	e.put(t, "a", "a1")
	e.put(t, "b", "b1")
	e.put(t, "c", "c1")
	e.del(t, "b")
	drops := dropFirst(e.nc, e.subject("c"))
	keys, err := e.kv.Keys(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if drops.Load() == 0 {
		t.Fatal("fault not injected")
	}
	if want := []string{"a", "c"}; !slices.Equal(keys, want) {
		t.Fatalf("Keys() = %v, want %v (b is deleted)", keys, want)
	}
}

// KV-2 (fast flavor): the first update of an UpdatesOnly watcher is lost;
// jsi.sseq is still 0, so the reset starts at stream sequence 1 and the
// watcher replays values that existed before it was created. Model:
// KV2_reset_from_one_bug.cfg (NoStaleEntry). The server-restart flavor
// needs no test hook: TestKV2UpdatesOnlyReplaysHistoryAfterServerRestart.
func TestKV2UpdatesOnlyReplaysHistoryAfterLostFirstUpdate(t *testing.T) {
	harness.KnownBug(t, "KV-2")
	e := newEnv(t, 1)
	e.put(t, "a", "a1")
	e.put(t, "b", "b1")
	w, err := e.kv.WatchAll(context.Background(), jetstream.UpdatesOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	drops := dropFirst(e.nc, e.subject("c"))
	e.put(t, "c", "c1") // rev 3, lost
	e.put(t, "d", "d1") // rev 4, reveals the gap
	got, _ := collect(w, 10, 2*time.Second)
	if drops.Load() == 0 {
		t.Fatal("fault not injected")
	}
	want := "[c@3=c1 d@4=d1]"
	if fmtRecs(got) != want {
		t.Fatalf("UpdatesOnly watcher delivered %s, want %s", fmtRecs(got), want)
	}
}

// KV-2, ResumeFromRevision flavor: nothing delivered yet, a gap restarts the
// consumer at sequence 1, below the requested revision.
func TestKV2ResumeFromRevisionReplaysOlderRevisions(t *testing.T) {
	harness.KnownBug(t, "KV-2")
	e := newEnv(t, 5)
	e.put(t, "a", "a1") // rev 1
	e.put(t, "b", "b1") // rev 2
	w, err := e.kv.WatchAll(context.Background(), jetstream.ResumeFromRevision(3))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	got, ok := collectUntilMarker(w, 2*time.Second)
	if !ok || len(got) != 1 {
		t.Fatalf("expected only the marker, got %s", fmtRecs(got))
	}
	drops := dropFirst(e.nc, e.subject("c"))
	e.put(t, "c", "c1") // rev 3, lost
	e.put(t, "d", "d1") // rev 4
	got, _ = collect(w, 10, 2*time.Second)
	if drops.Load() == 0 {
		t.Fatal("fault not injected")
	}
	for _, r := range got {
		if r.rev < 3 {
			t.Fatalf("revision %s is below ResumeFromRevision(3); got %s", r, fmtRecs(got))
		}
	}
}

// KV-4: Keys()/History() treat Updates() being closed (their ctx ended and
// js.Subscribe's context goroutine unsubscribed, js.go:2068-2073) like the
// end of the listing and return what they have with a nil error, or
// ErrNoKeysFound. Model: KV4_keys_ctx_bug.cfg (KeysNoSilentTruncation).
func TestKV4KeysContextCancelSilentlyTruncates(t *testing.T) {
	harness.KnownBug(t, "KV-4")
	e := newEnv(t, 1)
	for i := 0; i < 50; i++ {
		e.put(t, keyN(i), "v")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Hold the readLoop on key 25 long enough for Keys() to consume the
	// first 25 entries, cancel, and let the unsubscribe land.
	e.nc.AddMsgFilter(e.subject(keyN(25)), func(m *nats.Msg) *nats.Msg {
		time.Sleep(100 * time.Millisecond)
		cancel()
		time.Sleep(200 * time.Millisecond)
		return m
	})
	keys, err := e.kv.Keys(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Keys() after cancel = %d keys, err=%v; want context.Canceled", len(keys), err)
	}
}
