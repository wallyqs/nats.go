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

// Model-based stateful test derived from models/KVWatch. A reference copy
// of the bucket's stream (seq -> key, op, live, with the History limit,
// rollup purges and removals) is kept next to the real bucket; zizek drives
// the rules below, which are the model's actions, and after every rule the
// model's invariants are checked against what each real watcher delivered:
//
//   rule               TLA+ action(s)
//   put / del / purge  Put, Del(k, "DEL"), Del(k, "PURGE")
//   removeKey          RemoveKey (stream purge by subject, no marker)
//   expire             Expire (oldest stored message purged, like MaxAge);
//                      not in the default rule set, see KV-9
//   watch              SetupStart .. SetupPending (+ cb/server/reader steps)
//   stop               WatchStop (the collector keeps draining)
//   keys / listKeys    KeysRead/KeysReturn, Lister*/ListUser*
//   history            KeysRead/KeysReturn with mode ALL on one key
//   dropNext           NetDrop (internal_testing only, see faults_test)
//
// Invariants (names as in KVWatch.tla): MarkerAtMostOnce,
// NoMarkerUpdatesOnly, MarkerComplete, RevisionOrder, NoStaleEntry,
// NoLostUpdate, Converged + NoStuckInit (on settled state), NoLeakAfterStop
// (Updates() closes after Stop), KeysSound (+ exact completeness, since no
// write runs concurrently with Keys here).

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/modeltest/harness"
	zizek "github.com/wallyqs/zizek.go"
)

var modelKeys = []string{"a", "b", "c"}

// ---- reference stream ----

type refMsg struct {
	key  string
	op   jetstream.KeyValueOp
	live bool
}

type refStream struct {
	history int
	msgs    []refMsg // index = seq-1
}

func (r *refStream) last() uint64 { return uint64(len(r.msgs)) }

func (r *refStream) liveOf(k string) []uint64 {
	var out []uint64
	for i, m := range r.msgs {
		if m.live && m.key == k {
			out = append(out, uint64(i+1))
		}
	}
	return out
}

func (r *refStream) latest(k string) uint64 {
	l := r.liveOf(k)
	if len(l) == 0 {
		return 0
	}
	return l[len(l)-1]
}

func (r *refStream) msg(seq uint64) refMsg { return r.msgs[seq-1] }

func (r *refStream) appendMsg(k string, op jetstream.KeyValueOp) {
	if op == jetstream.KeyValuePurge {
		for i := range r.msgs {
			if r.msgs[i].key == k {
				r.msgs[i].live = false
			}
		}
	}
	r.msgs = append(r.msgs, refMsg{key: k, op: op, live: true})
	if l := r.liveOf(k); len(l) > r.history {
		r.msgs[l[0]-1].live = false
	}
}

func isDel(op jetstream.KeyValueOp) bool {
	return op == jetstream.KeyValueDelete || op == jetstream.KeyValuePurge
}

// ---- watchers ----

type watchMode int

const (
	modeLPS watchMode = iota
	modeAll
	modeNew
	modeResume
)

func (m watchMode) String() string { return [...]string{"LPS", "ALL", "NEW", "RESUME"}[m] }

type watcherState struct {
	id      int
	mode    watchMode
	resume  uint64
	ign     bool
	filter  []string // nil = all keys
	w       jetstream.KeyWatcher
	stopped bool

	// snapshot at creation (the model's snap)
	snapLatest map[string]uint64
	snapLast   uint64
	initSet    map[uint64]bool

	mu     sync.Mutex
	got    []rec
	closed bool
	done   chan struct{}
}

func (ws *watcherState) watches(k string) bool {
	return ws.filter == nil || slices.Contains(ws.filter, k)
}

func (ws *watcherState) floor(k string) uint64 {
	switch ws.mode {
	case modeLPS:
		return ws.snapLatest[k]
	case modeNew:
		return ws.snapLast + 1
	case modeResume:
		return ws.resume
	}
	return 0
}

func (ws *watcherState) collect() {
	defer close(ws.done)
	for e := range ws.w.Updates() {
		ws.mu.Lock()
		ws.got = append(ws.got, toRec(e))
		ws.mu.Unlock()
	}
	ws.mu.Lock()
	ws.closed = true
	ws.mu.Unlock()
}

func (ws *watcherState) snapshot() []rec {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return slices.Clone(ws.got)
}

func (ws *watcherState) String() string {
	f := "*"
	if ws.filter != nil {
		f = fmt.Sprint(ws.filter)
	}
	return fmt.Sprintf("w%d(%s ign=%v filter=%s resume=%d)", ws.id, ws.mode, ws.ign, f, ws.resume)
}

// ---- machine state ----

type kvMachine struct {
	t      testing.TB
	ctx    context.Context
	srv    *harness.Server
	wnc    *nats.Conn // watchers (message filters go here)
	kv     jetstream.KeyValue
	wkv    jetstream.KeyValue // same bucket through the watcher connection
	stream jetstream.Stream
	bucket string
	ref    *refStream

	watchers []*watcherState
	nextID   int
	settle   time.Duration
}

func (s *kvMachine) subject(k string) string { return "$KV." + s.bucket + "." + k }

func (s *kvMachine) drawKey(tc *zizek.TestCase) (string, error) {
	return zizek.Draw(tc, zizek.SampledFrom(modelKeys))
}

func (s *kvMachine) put(tc *zizek.TestCase) error {
	k, err := s.drawKey(tc)
	if err != nil {
		return err
	}
	val := fmt.Sprintf("%s%d", k, len(s.ref.msgs)+1)
	rev, err := s.kv.PutString(s.ctx, k, val)
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	s.ref.appendMsg(k, jetstream.KeyValuePut)
	tc.Note(fmt.Sprintf("put %s -> rev %d", k, rev))
	if rev != s.ref.last() {
		return fmt.Errorf("put %s returned rev %d, reference has %d", k, rev, s.ref.last())
	}
	return nil
}

func (s *kvMachine) del(tc *zizek.TestCase) error {
	k, err := s.drawKey(tc)
	if err != nil {
		return err
	}
	purge, err := tc.Boolean()
	if err != nil {
		return err
	}
	if purge {
		err = s.kv.Purge(s.ctx, k)
		s.ref.appendMsg(k, jetstream.KeyValuePurge)
	} else {
		err = s.kv.Delete(s.ctx, k)
		s.ref.appendMsg(k, jetstream.KeyValueDelete)
	}
	tc.Note(fmt.Sprintf("delete %s (purge=%v) -> rev %d", k, purge, s.ref.last()))
	return err
}

// removeKey: every stored message of k goes, no marker (PurgeDeletes with
// keep=0, stream purge by subject).
func (s *kvMachine) removeKey(tc *zizek.TestCase) error {
	k, err := s.drawKey(tc)
	if err != nil {
		return err
	}
	tc.Assume(len(s.ref.liveOf(k)) > 0)
	if err := s.stream.Purge(s.ctx, jetstream.WithPurgeSubject(s.subject(k))); err != nil {
		return fmt.Errorf("purge subject: %w", err)
	}
	for _, seq := range s.ref.liveOf(k) {
		s.ref.msgs[seq-1].live = false
	}
	tc.Note(fmt.Sprintf("removeKey %s", k))
	return nil
}

// expire: the oldest stored message goes (MaxAge).
func (s *kvMachine) expire(tc *zizek.TestCase) error {
	var oldest uint64
	for i, m := range s.ref.msgs {
		if m.live {
			oldest = uint64(i + 1)
			break
		}
	}
	tc.Assume(oldest > 0)
	// KV streams deny message deletes; purging below oldest+1 removes
	// exactly the oldest stored message, as MaxAge would.
	if err := s.stream.Purge(s.ctx, jetstream.WithPurgeSequence(oldest+1)); err != nil {
		return fmt.Errorf("purge up to %d: %w", oldest, err)
	}
	s.ref.msgs[oldest-1].live = false
	tc.Note(fmt.Sprintf("expire seq %d", oldest))
	return nil
}

func (s *kvMachine) watch(tc *zizek.TestCase) error {
	tc.Assume(len(s.activeWatchers()) < 3)
	m, err := tc.Integer(0, 3, 0)
	if err != nil {
		return err
	}
	ws := &watcherState{id: s.nextID, mode: watchMode(m), done: make(chan struct{})}
	s.nextID++
	if ws.ign, err = tc.Boolean(); err != nil {
		return err
	}
	f, err := tc.Integer(0, 2, 0)
	if err != nil {
		return err
	}
	switch f {
	case 1:
		ws.filter = []string{"a"}
	case 2:
		ws.filter = []string{"a", "b"}
	}
	var opts []jetstream.WatchOpt
	switch ws.mode {
	case modeAll:
		opts = append(opts, jetstream.IncludeHistory())
	case modeNew:
		opts = append(opts, jetstream.UpdatesOnly())
	case modeResume:
		r, err := tc.Integer(1, int64(s.ref.last())+2, 1)
		if err != nil {
			return err
		}
		ws.resume = uint64(r)
		opts = append(opts, jetstream.ResumeFromRevision(ws.resume))
	}
	if ws.ign {
		opts = append(opts, jetstream.IgnoreDeletes())
	}
	// Snapshot: writes are synchronous in this test, so the stream at
	// consumer creation is the reference as it is now.
	ws.snapLast = s.ref.last()
	ws.snapLatest = map[string]uint64{}
	ws.initSet = map[uint64]bool{}
	for _, k := range modelKeys {
		if !ws.watches(k) {
			continue
		}
		ws.snapLatest[k] = s.ref.latest(k)
		var owed []uint64
		switch ws.mode {
		case modeLPS:
			if l := s.ref.latest(k); l > 0 {
				owed = []uint64{l}
			}
		case modeAll:
			owed = s.ref.liveOf(k)
		case modeResume:
			for _, seq := range s.ref.liveOf(k) {
				if seq >= ws.resume {
					owed = append(owed, seq)
				}
			}
		}
		for _, seq := range owed {
			if !(ws.ign && isDel(s.ref.msg(seq).op)) {
				ws.initSet[seq] = true
			}
		}
	}
	if ws.filter == nil {
		ws.w, err = s.wkv.WatchAll(s.ctx, opts...)
	} else {
		ws.w, err = s.wkv.WatchFiltered(s.ctx, slices.Clone(ws.filter), opts...)
	}
	if err != nil {
		return fmt.Errorf("watch: %w", err)
	}
	go ws.collect()
	s.watchers = append(s.watchers, ws)
	tc.Note(fmt.Sprintf("watch %s at rev %d", ws, ws.snapLast))
	// Writes racing the initial delivery (the snapshot above stays exact:
	// the consumer exists once Watch returns).
	n, err := tc.Integer(0, 3, 0)
	if err != nil {
		return err
	}
	for range n {
		del, err := tc.Weighted(0.25)
		if err != nil {
			return err
		}
		if del {
			err = s.del(tc)
		} else {
			err = s.put(tc)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *kvMachine) activeWatchers() []*watcherState {
	var out []*watcherState
	for _, ws := range s.watchers {
		if !ws.stopped {
			out = append(out, ws)
		}
	}
	return out
}

func (s *kvMachine) stop(tc *zizek.TestCase) error {
	act := s.activeWatchers()
	tc.Assume(len(act) > 0)
	i, err := tc.Integer(0, int64(len(act)-1), 0)
	if err != nil {
		return err
	}
	ws := act[i]
	ws.stopped = true
	tc.Note(fmt.Sprintf("stop %s", ws))
	if err := ws.w.Stop(); err != nil {
		return fmt.Errorf("stop: %w", err)
	}
	// NoLeakAfterStop: with a reader draining, Updates() closes.
	select {
	case <-ws.done:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("NoLeakAfterStop: %s: Updates() not closed 5s after Stop", ws)
	}
	return nil
}

func (s *kvMachine) liveKeys(filter func(string) bool) []string {
	var out []string
	for _, k := range modelKeys {
		if l := s.ref.latest(k); l > 0 && !isDel(s.ref.msg(l).op) && filter(k) {
			out = append(out, k)
		}
	}
	return out
}

// callCtx bounds a Keys/ListKeys/History call: they block until the init
// marker, so a stuck marker shows up as this context ending.
func (s *kvMachine) callCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(s.ctx, s.settle)
}

func (s *kvMachine) keys(tc *zizek.TestCase) error {
	want := s.liveKeys(func(string) bool { return true })
	ctx, cancel := s.callCtx()
	defer cancel()
	got, err := s.wkv.Keys(ctx)
	tc.Note(fmt.Sprintf("keys -> %v, %v", got, err))
	if ctx.Err() != nil {
		return fmt.Errorf("NoStuckInit: Keys() did not finish in %v (returned %v, %v)", s.settle, got, err)
	}
	if len(want) == 0 {
		if !errors.Is(err, jetstream.ErrNoKeysFound) {
			return fmt.Errorf("KeysSound: Keys() = %v, %v on an empty bucket", got, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("keys: %w", err)
	}
	if !slices.Equal(got, want) {
		return fmt.Errorf("KeysSound: Keys() = %v, want %v", got, want)
	}
	return nil
}

func (s *kvMachine) listKeys(tc *zizek.TestCase) error {
	f, err := tc.Boolean()
	if err != nil {
		return err
	}
	var kl jetstream.KeyLister
	filter := func(string) bool { return true }
	ctx, cancel := s.callCtx()
	defer cancel()
	if f {
		filter = func(k string) bool { return k != "c" }
		kl, err = s.wkv.ListKeysFiltered(ctx, "a", "b")
	} else {
		kl, err = s.wkv.ListKeys(ctx)
	}
	if err != nil {
		return fmt.Errorf("listKeys: %w", err)
	}
	var got []string
	for k := range kl.Keys() {
		got = append(got, k)
	}
	if ctx.Err() != nil {
		return fmt.Errorf("NoStuckInit: ListKeys did not finish in %v, got %v", s.settle, got)
	}
	sort.Strings(got)
	want := s.liveKeys(filter)
	tc.Note(fmt.Sprintf("listKeys(filtered=%v) -> %v", f, got))
	if !slices.Equal(got, want) {
		return fmt.Errorf("KeysSound: ListKeys(filtered=%v) = %v, want %v", f, got, want)
	}
	return nil
}

func (s *kvMachine) history(tc *zizek.TestCase) error {
	k, err := s.drawKey(tc)
	if err != nil {
		return err
	}
	ctx, cancel := s.callCtx()
	defer cancel()
	ents, err := s.wkv.History(ctx, k)
	if ctx.Err() != nil {
		return fmt.Errorf("NoStuckInit: History(%s) did not finish in %v", k, s.settle)
	}
	want := s.ref.liveOf(k)
	tc.Note(fmt.Sprintf("history %s -> %d entries, %v", k, len(ents), err))
	if len(want) == 0 {
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("History(%s) = %d entries, %v; want ErrKeyNotFound", k, len(ents), err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("history: %w", err)
	}
	var got []uint64
	for _, e := range ents {
		got = append(got, e.Revision())
	}
	if !slices.Equal(got, want) {
		return fmt.Errorf("History(%s) revisions %v, want %v", k, got, want)
	}
	return nil
}

// ---- invariants ----

// settled waits until every live watcher has delivered the latest revision
// of every key it should see (Converged) and, unless UpdatesOnly, the
// marker (NoStuckInit). Writes are synchronous, so the server has
// everything already; only delivery is pending.
func (s *kvMachine) settled(_ *zizek.TestCase) error {
	for _, ws := range s.activeWatchers() {
		var why string
		ok := harness.Eventually(s.settle, func() bool {
			why = s.converged(ws, ws.snapshot())
			return why == ""
		})
		if !ok {
			return fmt.Errorf("%s: %s; delivered %s", ws, why, fmtRecs(ws.snapshot()))
		}
	}
	return nil
}

func (s *kvMachine) converged(ws *watcherState, got []rec) string {
	marker := slices.IndexFunc(got, func(r rec) bool { return r.marker })
	if ws.mode != modeNew && marker < 0 {
		return "NoStuckInit: no init marker"
	}
	maxRev := map[string]uint64{}
	for _, r := range got {
		if !r.marker && r.rev > maxRev[r.key] {
			maxRev[r.key] = r.rev
		}
	}
	for _, k := range modelKeys {
		if !ws.watches(k) {
			continue
		}
		l := s.ref.latest(k)
		switch {
		case l == 0, l < ws.floor(k):
		case ws.mode == modeNew && l <= ws.snapLast:
		case ws.ign && isDel(s.ref.msg(l).op):
		case maxRev[k] >= l:
		default:
			return fmt.Sprintf("Converged: latest %s@%d not delivered", k, l)
		}
	}
	return ""
}

// safety checks what was delivered so far against the model's safety
// invariants.
func (s *kvMachine) safety(_ *zizek.TestCase) error {
	for _, ws := range s.watchers {
		got := ws.snapshot()
		markers := 0
		var lastRev uint64
		sent := map[uint64]bool{}
		maxRev := map[string]uint64{}
		for i, r := range got {
			if r.marker {
				markers++
				if ws.mode == modeNew {
					return fmt.Errorf("%s: NoMarkerUpdatesOnly: %s", ws, fmtRecs(got))
				}
				if markers > 1 {
					return fmt.Errorf("%s: MarkerAtMostOnce: %s", ws, fmtRecs(got))
				}
				for seq := range ws.initSet {
					m := s.ref.msg(seq)
					if sent[seq] || (ws.mode == modeLPS && maxRev[m.key] > seq) {
						continue
					}
					if !m.live {
						continue // removed since; cannot tell when, accept
					}
					return fmt.Errorf("%s: MarkerComplete: marker at %d before %s@%d; %s",
						ws, i, m.key, seq, fmtRecs(got))
				}
				continue
			}
			if !ws.watches(r.key) {
				return fmt.Errorf("%s: foreign key %s", ws, r)
			}
			if r.rev <= lastRev {
				return fmt.Errorf("%s: RevisionOrder: %s", ws, fmtRecs(got))
			}
			if r.rev < ws.floor(r.key) {
				return fmt.Errorf("%s: NoStaleEntry: %s below %d; %s", ws, r, ws.floor(r.key), fmtRecs(got))
			}
			if ws.ign && isDel(r.op) {
				return fmt.Errorf("%s: IgnoreDeletes delivered %s", ws, r)
			}
			ref := s.ref.msg(r.rev)
			if ref.key != r.key || ref.op != r.op {
				return fmt.Errorf("%s: entry %s does not match the stream (%s %v)", ws, r, ref.key, ref.op)
			}
			// NoLostUpdate: post-creation messages skipped although still stored.
			if !ws.stopped {
				for seq := lastRev + 1; seq < r.rev; seq++ {
					m := s.ref.msg(seq)
					if seq > ws.snapLast && seq >= ws.floor(m.key) && m.live && ws.watches(m.key) &&
						!(ws.ign && isDel(m.op)) {
						return fmt.Errorf("%s: NoLostUpdate: %s@%d skipped; %s", ws, m.key, seq, fmtRecs(got))
					}
				}
			}
			lastRev = r.rev
			sent[r.rev] = true
			maxRev[r.key] = r.rev
		}
	}
	return nil
}

func (s *kvMachine) cleanup() {
	for _, ws := range s.watchers {
		if !ws.stopped {
			ws.w.Stop()
		}
	}
	s.wnc.Close()
}

// ---- driver ----

type kvFixture struct {
	t      testing.TB
	srv    *harness.Server
	nc     *nats.Conn
	js     jetstream.JetStream
	bucket atomic.Int64

	statsMu sync.Mutex
	stats   map[string]int // successful rule applications, for the log
}

func (f *kvFixture) counted(r zizek.Rule[kvMachine]) zizek.Rule[kvMachine] {
	apply := r.Apply
	r.Apply = func(s *kvMachine, tc *zizek.TestCase) error {
		err := apply(s, tc)
		if err == nil {
			f.statsMu.Lock()
			f.stats[r.Name]++
			f.statsMu.Unlock()
		}
		return err
	}
	return r
}

func (f *kvFixture) logStats() {
	f.statsMu.Lock()
	defer f.statsMu.Unlock()
	f.t.Logf("rule applications: %v", f.stats)
}

func newFixture(t testing.TB) *kvFixture {
	s := harness.RunServer(t, true)
	nc, err := nats.Connect(s.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return &kvFixture{t: t, srv: s, nc: nc, js: js, stats: map[string]int{}}
}

func (f *kvFixture) newMachine(tc *zizek.TestCase, settle time.Duration) (*kvMachine, error) {
	hist, err := tc.Integer(1, 3, 1)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	name := fmt.Sprintf("M%d", f.bucket.Add(1))
	kv, err := f.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: name, History: uint8(hist)})
	if err != nil {
		return nil, err
	}
	stream, err := f.js.Stream(ctx, "KV_"+name)
	if err != nil {
		return nil, err
	}
	wnc, err := nats.Connect(f.srv.URL())
	if err != nil {
		return nil, err
	}
	wjs, _ := jetstream.New(wnc)
	wkv, err := wjs.KeyValue(ctx, name)
	if err != nil {
		wnc.Close()
		return nil, err
	}
	tc.Note(fmt.Sprintf("bucket %s history=%d", name, hist))
	return &kvMachine{t: f.t, ctx: ctx, srv: f.srv, wnc: wnc, kv: kv, wkv: wkv,
		stream: stream, bucket: name, ref: &refStream{history: int(hist)}, settle: settle}, nil
}

func (f *kvFixture) property(extra []zizek.Rule[kvMachine], steps int, settle time.Duration) func(tc *zizek.TestCase) error {
	return func(tc *zizek.TestCase) error {
		s, err := f.newMachine(tc, settle)
		if err != nil {
			return err
		}
		defer func() {
			s.cleanup()
			f.js.DeleteKeyValue(context.Background(), s.bucket)
		}()
		rules := []zizek.Rule[kvMachine]{
			{Name: "put", Apply: (*kvMachine).put},
			{Name: "del", Apply: (*kvMachine).del},
			{Name: "removeKey", Apply: (*kvMachine).removeKey},
			{Name: "watch", Apply: (*kvMachine).watch},
			{Name: "stop", Apply: (*kvMachine).stop},
			{Name: "keys", Apply: (*kvMachine).keys},
			{Name: "listKeys", Apply: (*kvMachine).listKeys},
			{Name: "history", Apply: (*kvMachine).history},
		}
		rules = append(rules, extra...)
		for i := range rules {
			rules[i] = f.counted(rules[i])
		}
		machine := zizek.Machine[kvMachine]{
			Rules: rules,
			Invariants: []zizek.Rule[kvMachine]{
				{Name: "settled", Apply: (*kvMachine).settled},
				{Name: "safety", Apply: (*kvMachine).safety},
			},
		}
		return zizek.RunSteps(s, machine, tc, uint64(steps))
	}
}

// TestKVWatchModel runs the model's actions without client-side faults
// against the real bucket and watchers; it passes on the current code.
// Removals happen between rules, i.e. after delivery has settled, so KV-3's
// window is not reachable here. "expire" is left out: combined with
// removeKey it trips a nats-server file store accounting bug (KV-9) that
// this test found; TestKV9 pins it.
func TestKVWatchModel(t *testing.T) {
	f := newFixture(t)
	err := zizek.Check(f.property(nil, 30, 3*time.Second), zizek.Config{Examples: 100, Seed: 7})
	f.logStats()
	if err != nil {
		t.Fatal(err)
	}
}
