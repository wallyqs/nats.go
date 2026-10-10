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

// Package kvwatch holds the tests derived from models/KVWatch: the
// JetStream KeyValue watcher (jetstream/kv.go WatchFiltered and the
// helpers built on it) running over the legacy ordered push consumer
// (js.go). See models/KVWatch/README.md for the model and the findings.
package kvwatch

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/modeltest/harness"
)

// env is one server, one client connection and one bucket.
type env struct {
	srv    *harness.Server
	nc     *nats.Conn
	js     jetstream.JetStream
	kv     jetstream.KeyValue
	bucket string
}

func newEnv(t testing.TB, history uint8, opts ...nats.Option) *env {
	t.Helper()
	s := harness.RunServer(t, true)
	return newEnvOn(t, s, s.URL(), history, opts...)
}

func newEnvOn(t testing.TB, s *harness.Server, url string, history uint8, opts ...nats.Option) *env {
	t.Helper()
	opts = append([]nats.Option{nats.MaxReconnects(-1), nats.ReconnectWait(20 * time.Millisecond)}, opts...)
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "B", History: history})
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return &env{srv: s, nc: nc, js: js, kv: kv, bucket: "B"}
}

// subject is the stream subject that holds key.
func (e *env) subject(key string) string { return "$KV." + e.bucket + "." + key }

func (e *env) put(t testing.TB, key, val string) uint64 {
	t.Helper()
	rev, err := e.kv.PutString(context.Background(), key, val)
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return rev
}

func (e *env) del(t testing.TB, key string) {
	t.Helper()
	if err := e.kv.Delete(context.Background(), key); err != nil {
		t.Fatalf("delete %s: %v", key, err)
	}
}

// rec is what a watcher delivered: an entry, or the init-done marker.
type rec struct {
	marker bool
	key    string
	rev    uint64
	op     jetstream.KeyValueOp
	val    string
}

func (r rec) String() string {
	if r.marker {
		return "<nil>"
	}
	switch r.op {
	case jetstream.KeyValueDelete:
		return fmt.Sprintf("%s@%d:DEL", r.key, r.rev)
	case jetstream.KeyValuePurge:
		return fmt.Sprintf("%s@%d:PURGE", r.key, r.rev)
	}
	v := r.val
	if len(v) > 8 {
		v = v[:8] + "..."
	}
	return fmt.Sprintf("%s@%d=%s", r.key, r.rev, v)
}

func fmtRecs(rs []rec) string {
	s := make([]string, len(rs))
	for i, r := range rs {
		s[i] = r.String()
	}
	return "[" + strings.Join(s, " ") + "]"
}

func toRec(e jetstream.KeyValueEntry) rec {
	if e == nil {
		return rec{marker: true}
	}
	return rec{key: e.Key(), rev: e.Revision(), op: e.Operation(), val: string(e.Value())}
}

// collect reads from w until it has n records, the channel closes, or the
// timeout passes without a new record.
func collect(w jetstream.KeyWatcher, n int, idle time.Duration) (out []rec, closed bool) {
	for len(out) < n {
		select {
		case e, ok := <-w.Updates():
			if !ok {
				return out, true
			}
			out = append(out, toRec(e))
		case <-time.After(idle):
			return out, false
		}
	}
	return out, false
}

// collectUntilMarker reads until the init-done marker or idle timeout.
func collectUntilMarker(w jetstream.KeyWatcher, idle time.Duration) (out []rec, gotMarker bool) {
	for {
		select {
		case e, ok := <-w.Updates():
			if !ok {
				return out, false
			}
			r := toRec(e)
			out = append(out, r)
			if r.marker {
				return out, true
			}
		case <-time.After(idle):
			return out, false
		}
	}
}

func keyN(i int) string { return fmt.Sprintf("k%03d", i) }
