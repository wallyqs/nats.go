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

// Deterministic regression tests for the model's counterexamples that need
// the nats.go test hooks (testing_internal.go): AddMsgFilter drops or holds
// a delivered message exactly where the TLA+ counterexample does.

import (
	"context"
	"errors"
	"fmt"
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
// initial values made the ordered consumer restart with
// DeliverByStartSequence(jsi.sseq+1), which replays every revision after
// the last delivered one: superseded revisions were handed out as initial
// values and the marker was counted against them, arriving before the real
// latest value. Fixed: the consumer is recreated with DeliverLastPerSubject
// while that set is being delivered (js.go) and the watcher re-counts what
// is owed (kv.go). Model: KV1_stale_reset_bug.cfg (NoStaleEntry),
// KVWatch_fixed_lps.cfg.
func TestKV1ResetDuringInitialPhaseDeliversStaleRevision(t *testing.T) {
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

// KV-1, Keys flavor: the replay handed Keys() (IgnoreDeletes) the old Put
// of a key whose latest revision is a delete. Model:
// KV1_keys_deleted_bug.cfg (KeysSound).
func TestKV1KeysReturnsDeletedKeyAfterReset(t *testing.T) {
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
// jsi.sseq was still 0, so the reset started at stream sequence 1 and the
// watcher replayed values that existed before it was created. Fixed:
// jsi.sseq starts at the consumer's Delivered.Stream (js.go). Model:
// KV2_reset_from_one_bug.cfg (NoStaleEntry). The server-restart flavor
// needs no test hook: TestKV2UpdatesOnlyReplaysHistoryAfterServerRestart.
func TestKV2UpdatesOnlyReplaysHistoryAfterLostFirstUpdate(t *testing.T) {
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

// KV-2, ResumeFromRevision flavor: nothing delivered yet, a gap restarted
// the consumer at sequence 1, below the requested revision.
func TestKV2ResumeFromRevisionReplaysOlderRevisions(t *testing.T) {
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

// KV-4: Keys()/History() treated Updates() being closed (their ctx ended
// and js.Subscribe's context goroutine unsubscribed) like the end of the
// listing and returned what they had with a nil error, or ErrNoKeysFound.
// Fixed: they return the context's error. Model: KV4_keys_ctx_bug.cfg
// (KeysNoSilentTruncation).
func TestKV4KeysContextCancelSilentlyTruncates(t *testing.T) {
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

// KV-1 for a plain legacy ordered consumer (js.Subscribe with
// OrderedConsumer and DeliverLastPerSubject, no KV): a gap while the last
// message per subject is being delivered must not replay superseded
// messages, and once that set is delivered a gap must resume by sequence
// again, without skipping any newer message.
func TestKV1LegacyOrderedLastPerSubjectReset(t *testing.T) {
	s := harness.RunServer(t, true)
	nc, err := nats.Connect(s.URL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "S", Subjects: []string{"foo.*"}, MaxMsgsPerSubject: 5}); err != nil {
		t.Fatal(err)
	}
	pub := func(subj, data string) {
		t.Helper()
		if _, err := js.Publish(subj, []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	pub("foo.a", "a1") // 1
	pub("foo.b", "b1") // 2 (superseded)
	pub("foo.c", "c1") // 3
	pub("foo.b", "b2") // 4
	// Lose the first delivery of foo.c (seq 3) and later of the first a2.
	var dropC, dropA atomic.Int32
	nc.AddMsgFilter("foo.c", func(m *nats.Msg) *nats.Msg {
		if m.Reply != "" && dropC.Add(1) == 1 {
			return nil
		}
		return m
	})
	nc.AddMsgFilter("foo.a", func(m *nats.Msg) *nats.Msg {
		if m.Reply != "" && string(m.Data) == "a2" && dropA.Add(1) == 1 {
			return nil
		}
		return m
	})
	got := make(chan string, 64)
	sub, err := js.Subscribe("foo.*", func(m *nats.Msg) {
		md, _ := m.Metadata()
		got <- fmt.Sprintf("%s@%d", m.Data, md.Sequence.Stream)
	}, nats.OrderedConsumer(), nats.DeliverLastPerSubject())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	read := func(n int) []string {
		t.Helper()
		var out []string
		for len(out) < n {
			select {
			case s := <-got:
				out = append(out, s)
			case <-time.After(15 * time.Second):
				t.Fatalf("got %v, want %d messages", out, n)
			}
		}
		return out
	}
	if g, want := read(3), []string{"a1@1", "c1@3", "b2@4"}; !slices.Equal(g, want) {
		t.Fatalf("last per subject after a gap: got %v, want %v", g, want)
	}
	// Past the initial set: every message, a gap resumes by sequence.
	pub("foo.a", "a2") // 5, lost
	pub("foo.a", "a3") // 6
	pub("foo.d", "d1") // 7
	if g, want := read(3), []string{"a2@5", "a3@6", "d1@7"}; !slices.Equal(g, want) {
		t.Fatalf("after the initial set: got %v, want %v", g, want)
	}
	if dropC.Load() == 0 || dropA.Load() == 0 {
		t.Fatal("faults not injected")
	}
	select {
	case s := <-got:
		t.Fatalf("unexpected extra message %s", s)
	case <-time.After(200 * time.Millisecond):
	}
}
