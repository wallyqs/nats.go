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

// Package pubasync holds the tests derived from models/PublishAsync: the
// JetStream asynchronous publisher (jetstream/publish.go PublishMsgAsync,
// handleAsyncReply, the ack timer, 503 retries, max-pending stalls,
// PublishAsyncComplete, resetPendingAcksOnReconnect and CleanupPublisher),
// plus the matching legacy js.go code where it differs. See
// models/PublishAsync/README.md for the model and the findings (PUB-n).
package pubasync

import (
	"context"
	"errors"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/modeltest/harness"
)

// Subjects used throughout:
//
//	s.>       bound to stream S: the server acks.
//	silent.>  a plain core subscriber that never replies: no ack, no 503,
//	          the future stays pending until timeout/reconnect/cleanup.
//	nostream.> nobody listens: the server answers 503 (no responders).
const streamName = "S"

type env struct {
	t     testing.TB
	srv   *harness.Server
	proxy *harness.Proxy
	dial  *gatedDialer
	nc    *nats.Conn // the publisher's connection, through the proxy
	side  *nats.Conn // direct connection for the silent subscriber
	js    jetstream.JetStream
}

func newEnv(t testing.TB, jsOpts []jetstream.JetStreamOpt, ncOpts ...nats.Option) *env {
	t.Helper()
	s := harness.RunServer(t, true)
	e := &env{t: t, srv: s, proxy: harness.NewProxy(t, s.Addr()), dial: &gatedDialer{}}

	side, err := nats.Connect(s.URL())
	if err != nil {
		t.Fatalf("side connect: %v", err)
	}
	t.Cleanup(side.Close)
	e.side = side
	if _, err := side.Subscribe("silent.>", func(*nats.Msg) {}); err != nil {
		t.Fatalf("silent sub: %v", err)
	}
	if err := side.Flush(); err != nil {
		t.Fatal(err)
	}
	sjs, err := jetstream.New(side)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := sjs.CreateStream(ctx, jetstream.StreamConfig{Name: streamName, Subjects: []string{"s.>"}}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	opts := append([]nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectWait(20 * time.Millisecond),
		nats.ReconnectJitter(0, 0),
		nats.SetCustomDialer(e.dial),
	}, ncOpts...)
	nc, err := nats.Connect(e.proxy.URL(), opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	e.nc = nc
	js, err := jetstream.New(nc, jsOpts...)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	e.js = js
	return e
}

// warmUp performs one acked publish so that the reply subscription and the
// reconnect listener exist (newAsyncReply, publish.go:438-462).
func (e *env) warmUp() {
	e.t.Helper()
	paf, err := e.js.PublishAsync("s.warmup", []byte("w"))
	if err != nil {
		e.t.Fatalf("warm-up publish: %v", err)
	}
	select {
	case <-paf.Ok():
	case err := <-paf.Err():
		e.t.Fatalf("warm-up publish failed: %v", err)
	case <-time.After(5 * time.Second):
		e.t.Fatalf("warm-up publish not acked")
	}
}

// cutAndAwaitReconnect drops the client's TCP connection at the proxy and
// waits for the client to be connected again.
func (e *env) cutAndAwaitReconnect() {
	e.t.Helper()
	before := e.nc.Stats().Reconnects
	e.proxy.Cut()
	if !harness.Eventually(10*time.Second, func() bool {
		return e.nc.Stats().Reconnects > before && e.nc.IsConnected()
	}) {
		e.t.Fatalf("client did not reconnect (status %v)", e.nc.Status())
	}
}

// gatedDialer is the client's CustomDialer. When armed, the next Dial
// signals entered and blocks until released, then fails. nats.go dials while
// holding the connection mutex (doReconnect -> createConn), so a parked dial
// parks every publish on nc.mu: that is how the tests open the windows the
// model found between registerPAF and PublishMsg deterministically.
type gatedDialer struct {
	mu      sync.Mutex
	armed   bool
	entered chan struct{}
	release chan struct{}
	d       net.Dialer
}

func (g *gatedDialer) Dial(network, address string) (net.Conn, error) {
	g.mu.Lock()
	if g.armed {
		g.armed = false
		entered, release := g.entered, g.release
		g.mu.Unlock()
		close(entered)
		<-release
		return nil, errors.New("pubasync: gated dial refused")
	}
	g.mu.Unlock()
	return g.d.Dial(network, address)
}

// arm makes the next dial park. It returns a channel closed once a dial is
// parked and a function that lets it fail.
func (g *gatedDialer) arm() (entered <-chan struct{}, release func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.armed = true
	g.entered = make(chan struct{})
	g.release = make(chan struct{})
	rel := g.release
	var once sync.Once
	return g.entered, func() { once.Do(func() { close(rel) }) }
}

// goroutineStacks returns the stack of every goroutine.
func goroutineStacks() []string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Split(string(buf[:n]), "\n\n")
		}
		buf = make([]byte, 2*len(buf))
	}
}

// countGoroutines counts goroutines whose stack contains every substring.
func countGoroutines(substrs ...string) int {
	n := 0
	for _, g := range goroutineStacks() {
		all := true
		for _, s := range substrs {
			if !strings.Contains(g, s) {
				all = false
				break
			}
		}
		if all {
			n++
		}
	}
	return n
}

// waitGoroutine waits until some goroutine's stack contains every substring.
func waitGoroutine(t testing.TB, what string, substrs ...string) {
	t.Helper()
	if !harness.Eventually(5*time.Second, func() bool { return countGoroutines(substrs...) > 0 }) {
		t.Fatalf("no goroutine reached %s (%v)", what, substrs)
	}
}

// within runs f and reports whether it returned before d. A call that does
// not return is left behind (it is blocked on a library mutex).
func within(d time.Duration, f func()) bool {
	done := make(chan struct{})
	go func() {
		f()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

func closedWithin(ch <-chan struct{}, d time.Duration) bool {
	select {
	case <-ch:
		return true
	case <-time.After(d):
		return false
	}
}

// handlerLog records MsgErrHandler calls per subject; calls whose subject
// is in block park until the returned release func runs.
type handlerLog struct {
	mu      sync.Mutex
	calls   map[string][]error
	block   map[string]chan struct{}
	entered map[string]chan struct{}
}

func newHandlerLog() *handlerLog {
	return &handlerLog{calls: map[string][]error{}, block: map[string]chan struct{}{}, entered: map[string]chan struct{}{}}
}

func (h *handlerLog) blockOn(subject string) (entered <-chan struct{}, release func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rel := make(chan struct{})
	ent := make(chan struct{})
	h.block[subject] = rel
	h.entered[subject] = ent
	var once sync.Once
	return ent, func() { once.Do(func() { close(rel) }) }
}

func (h *handlerLog) handler(_ jetstream.JetStream, m *nats.Msg, err error) {
	h.mu.Lock()
	h.calls[m.Subject] = append(h.calls[m.Subject], err)
	rel := h.block[m.Subject]
	ent := h.entered[m.Subject]
	delete(h.block, m.Subject)
	delete(h.entered, m.Subject)
	h.mu.Unlock()
	if rel != nil {
		close(ent)
		<-rel
	}
}

func (h *handlerLog) get(subject string) []error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]error(nil), h.calls[subject]...)
}
