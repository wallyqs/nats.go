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
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/modeltest/harness"
)

const (
	streamName = "S"
	subject    = "s.x"
	apiPrefix  = "RLY"
)

// relay sits between the ordered consumer and the JetStream API. The
// client's JetStream context uses the API prefix "RLY.", and the relay
// forwards each request on its own connection to "$JS.API." keeping the
// client's reply subject, so the server answers the client directly. A test
// can hold (and later release or drop) the next request whose API subject
// starts with a given prefix, e.g. "CONSUMER.CREATE." to park reset()
// inside CreateOrUpdateConsumer, which is the environment step "create in
// flight" of the models. Everything else passes through unchanged.
type relay struct {
	t   testing.TB
	nc  *nats.Conn
	sub *nats.Subscription

	mu    sync.Mutex
	holds []*holdRule
	log   []relayed
}

type relayed struct {
	at   time.Time
	subj string // API subject without prefix, e.g. CONSUMER.MSG.NEXT.S.name
}

type holdRule struct {
	prefix string
	ch     chan *held
	all    bool // hold every matching request, not just the next one
}

// held is a request the relay did not forward yet.
type held struct {
	r    *relay
	Subj string
	msg  *nats.Msg
	once sync.Once
}

// Release forwards the held request to the server.
func (h *held) Release() { h.once.Do(func() { h.r.forward(h.Subj, h.msg) }) }

// Drop discards the held request; the client's request times out.
func (h *held) Drop() { h.once.Do(func() {}) }

func newRelay(t testing.TB, url string) *relay {
	t.Helper()
	nc, err := nats.Connect(url, nats.MaxReconnects(-1), nats.ReconnectWait(10*time.Millisecond))
	if err != nil {
		t.Fatalf("relay connect: %v", err)
	}
	r := &relay{t: t, nc: nc}
	r.sub, err = nc.Subscribe(apiPrefix+".>", r.handle)
	if err != nil {
		t.Fatalf("relay subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("relay flush: %v", err)
	}
	t.Cleanup(nc.Close)
	return r
}

func (r *relay) handle(m *nats.Msg) {
	subj := strings.TrimPrefix(m.Subject, apiPrefix+".")
	r.mu.Lock()
	r.log = append(r.log, relayed{at: time.Now(), subj: subj})
	for i, h := range r.holds {
		if strings.HasPrefix(subj, h.prefix) {
			if !h.all {
				r.holds = append(r.holds[:i:i], r.holds[i+1:]...)
			}
			r.mu.Unlock()
			h.ch <- &held{r: r, Subj: subj, msg: m}
			return
		}
	}
	r.mu.Unlock()
	r.forward(subj, m)
}

func (r *relay) forward(subj string, m *nats.Msg) {
	out := &nats.Msg{Subject: "$JS.API." + subj, Reply: m.Reply, Header: m.Header, Data: m.Data}
	if err := r.nc.PublishMsg(out); err != nil {
		r.t.Logf("relay forward %s: %v", subj, err)
	}
}

// HoldNext holds the next request whose API subject starts with prefix.
func (r *relay) HoldNext(prefix string) <-chan *held {
	ch := make(chan *held, 1)
	r.mu.Lock()
	r.holds = append(r.holds, &holdRule{prefix: prefix, ch: ch})
	r.mu.Unlock()
	return ch
}

// HoldAll holds every request whose API subject starts with prefix until
// the returned stop function is called (requests held so far stay held).
func (r *relay) HoldAll(prefix string) (<-chan *held, func()) {
	ch := make(chan *held, 1024)
	rule := &holdRule{prefix: prefix, ch: ch, all: true}
	r.mu.Lock()
	r.holds = append(r.holds, rule)
	r.mu.Unlock()
	return ch, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		for i, h := range r.holds {
			if h == rule {
				r.holds = append(r.holds[:i:i], r.holds[i+1:]...)
				return
			}
		}
	}
}

// Since returns the API subjects relayed at or after t.
func (r *relay) Since(t time.Time) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, l := range r.log {
		if !l.at.Before(t) {
			out = append(out, l.subj)
		}
	}
	return out
}

func waitHeld(t testing.TB, ch <-chan *held, what string) *held {
	t.Helper()
	select {
	case h := <-ch:
		return h
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s to reach the relay", what)
		return nil
	}
}

// env is a JetStream server with a stream S on s.>, a client connection
// (optionally through a TCP proxy, optionally with its JetStream API
// requests going through the relay), and a second, direct connection used to play the environment
// (publish, delete the server-side consumer, steal messages).
type env struct {
	t     testing.TB
	srv   *harness.Server
	proxy *harness.Proxy
	rl    *relay
	nc    *nats.Conn          // the ordered consumer's connection
	js    jetstream.JetStream // the client's, through the relay if enabled
	admin *nats.Conn
	ajs   jetstream.JetStream // direct
	seq   uint64              // last published stream sequence
}

type envOpts struct {
	proxy bool // client connects through a harness.Proxy
	// relay routes the client's JetStream API requests through the relay.
	// Requests the relay forwards have a responder (the relay), so a pull
	// on a consumer that does not exist gets no 503 No Responders; tests
	// that depend on that status must not use the relay.
	relay bool
	conn  []nats.Option
}

func newEnv(t testing.TB, o envOpts) *env {
	t.Helper()
	e := &env{t: t, srv: harness.RunServer(t, true)}
	url := e.srv.URL()
	if o.proxy {
		e.proxy = harness.NewProxy(t, e.srv.Addr())
		url = e.proxy.URL()
	}
	if o.relay {
		e.rl = newRelay(t, e.srv.URL())
	}
	opts := append([]nats.Option{nats.MaxReconnects(-1), nats.ReconnectWait(10 * time.Millisecond)}, o.conn...)
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	e.nc = nc
	if o.relay {
		e.js, err = jetstream.NewWithAPIPrefix(nc, apiPrefix)
	} else {
		e.js, err = jetstream.New(nc)
	}
	if err != nil {
		t.Fatal(err)
	}
	if e.admin, err = nats.Connect(e.srv.URL(), nats.MaxReconnects(-1), nats.ReconnectWait(10*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.admin.Close)
	if e.ajs, err = jetstream.New(e.admin); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := e.ajs.CreateStream(ctx, jetstream.StreamConfig{Name: streamName, Subjects: []string{"s.>"}}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return e
}

// close tears the environment down early (the t.Cleanup hooks remain and
// are no-ops afterwards); the stateful tests build one env per example.
func (e *env) close() {
	e.nc.Close()
	e.admin.Close()
	if e.rl != nil {
		e.rl.nc.Close()
	}
	if e.proxy != nil {
		e.proxy.Close()
	}
	e.srv.Shutdown()
}

// publish appends n messages to the stream; message k carries payload "k".
func (e *env) publish(n int) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		ack, err := e.ajs.Publish(ctx, subject, []byte(fmt.Sprint(e.seq+1)))
		if err != nil {
			e.t.Fatalf("publish: %v", err)
		}
		e.seq = ack.Sequence
	}
}

func (e *env) ordered(cfg jetstream.OrderedConsumerConfig) jetstream.Consumer {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := e.js.OrderedConsumer(ctx, streamName, cfg)
	if err != nil {
		e.t.Fatalf("ordered consumer: %v", err)
	}
	return c
}

// consumerName is the name of the ordered consumer's current server-side
// consumer.
func consumerName(t testing.TB, c jetstream.Consumer) string {
	t.Helper()
	info := c.CachedInfo()
	if info == nil {
		t.Fatal("no cached info")
	}
	return info.Name
}

// deleteConsumer deletes a server-side consumer behind the client's back
// (environment action ExtDelete of the models).
func (e *env) deleteConsumer(name string) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.ajs.DeleteConsumer(ctx, streamName, name); err != nil {
		e.t.Fatalf("delete consumer %s: %v", name, err)
	}
}

func streamSeq(t testing.TB, m jetstream.Msg) uint64 {
	t.Helper()
	md, err := m.Metadata()
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	return md.Sequence.Stream
}

// blockedIn reports whether some goroutine is blocked acquiring a mutex
// inside a function whose name contains fn (e.g.
// "(*orderedSubscription).Stop"). Tests use it to know that a goroutine
// reached the statement a model trace names before taking the next step.
func blockedIn(fn string) bool {
	buf := make([]byte, 1<<22)
	n := runtime.Stack(buf, true)
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(g, fn) && strings.Contains(g, "sync.(*Mutex).Lock") {
			return true
		}
	}
	return false
}

func waitBlockedIn(t testing.TB, fn string) {
	t.Helper()
	if !harness.Eventually(10*time.Second, func() bool { return blockedIn(fn) }) {
		t.Fatalf("no goroutine blocked on a mutex in %s", fn)
	}
}

func recvWithin[T any](ch <-chan T, d time.Duration) (T, bool) {
	select {
	case v := <-ch:
		return v, true
	case <-time.After(d):
		var zero T
		return zero, false
	}
}
