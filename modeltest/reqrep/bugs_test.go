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

package reqrep

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/modeltest/harness"
)

func connect(t testing.TB, url string, opts ...nats.Option) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// echo starts a responder on subj that replies with the request payload.
func echo(t testing.TB, nc *nats.Conn, subj string) {
	t.Helper()
	if _, err := nc.Subscribe(subj, func(m *nats.Msg) { _ = m.Respond(m.Data) }); err != nil {
		t.Fatalf("subscribe %s: %v", subj, err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// ---------------------------------------------------------------------------
// REQ-1: createNewRequestAndSend registers respMap[token] before publish, and
// newRequest / requestWithContext return publish's error without deleting it
// (nats.go:4846, 4916-4919; context.go:60-63). Model: leak_bug.cfg.
// ---------------------------------------------------------------------------

func TestREQ1RespMapLeaksOnPublishError(t *testing.T) {
	harness.KnownBug(t, "REQ-1")
	s := harness.RunServer(t, false)
	nc := connect(t, s.URL())
	echo(t, nc, "svc")
	if _, err := nc.Request("svc", []byte("warm"), 2*time.Second); err != nil {
		t.Fatalf("warm-up request: %v", err)
	}

	big := make([]byte, nc.MaxPayload()+1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cases := []struct {
		name string
		do   func() error
		want error
	}{
		{"bad subject", func() error { _, err := nc.Request("bad subject", nil, time.Second); return err }, nats.ErrBadSubject},
		{"max payload", func() error { _, err := nc.Request("svc", big, time.Second); return err }, nats.ErrMaxPayload},
		{"ctx bad subject", func() error { _, err := nc.RequestWithContext(ctx, "bad\tsubject", nil); return err }, nats.ErrBadSubject},
	}
	for _, c := range cases {
		before := Peek(nc).RespMapLen
		if err := c.do(); !errors.Is(err, c.want) {
			t.Fatalf("%s: err = %v, want %v", c.name, err, c.want)
		}
		if after := Peek(nc).RespMapLen; after != before {
			t.Errorf("%s: respMap grew from %d to %d after a request that returned an error", c.name, before, after)
		}
	}
}

// The realistic shape of REQ-1: an application that keeps retrying a request
// while the client is disconnected gets ErrReconnectBufExceeded once the
// reconnect buffer is full, and every failed attempt leaves a channel in
// respMap for the lifetime of the connection - reconnecting does not clean
// them up.
func TestREQ1RespMapLeaksWhileReconnectBufferFull(t *testing.T) {
	harness.KnownBug(t, "REQ-1")
	s := harness.RunServer(t, false)
	p := harness.NewProxy(t, s.Addr())
	nc := connect(t, p.URL(), nats.ReconnectBufSize(256), nats.ReconnectWait(20*time.Millisecond),
		nats.MaxReconnects(-1))
	resp := connect(t, s.URL())
	echo(t, resp, "svc")
	if _, err := nc.Request("svc", []byte("warm"), 2*time.Second); err != nil {
		t.Fatalf("warm-up request: %v", err)
	}

	p.Block()
	if !harness.Eventually(5*time.Second, nc.IsReconnecting) {
		t.Fatalf("client did not notice the outage")
	}
	// Fill the reconnect buffer; from then on publish fails fast
	// (atLimitIfUsingPending, nats.go:4725).
	payload := make([]byte, 512)
	if err := nc.Publish("filler", payload); err != nil {
		t.Fatalf("filling the reconnect buffer: %v", err)
	}
	const attempts = 1000
	for i := 0; i < attempts; i++ {
		if _, err := nc.Request("svc", payload, time.Second); !errors.Is(err, nats.ErrReconnectBufExceeded) {
			t.Fatalf("attempt %d: err = %v, want ErrReconnectBufExceeded", i, err)
		}
	}
	p.Unblock()
	if !harness.Eventually(10*time.Second, nc.IsConnected) {
		t.Fatalf("client did not reconnect")
	}
	if _, err := nc.Request("svc", []byte("after"), 2*time.Second); err != nil {
		t.Fatalf("request after reconnect: %v", err)
	}
	if n := Peek(nc).RespMapLen; n != 0 {
		t.Fatalf("respMap holds %d entries with no request in flight (%d failed attempts during the outage)", n, attempts)
	}
}

// ---------------------------------------------------------------------------
// REQ-2: a reply's Msg.Sub is the shared mux subscription (processMsg sets
// m.Sub = sub, nats.go:3951). Unsubscribe / AutoUnsubscribe / Drain on it
// remove it from nc.subs (removeSub nats.go:5237), but nc.respMux is never
// reset, so createNewRequestAndSend (nats.go:4847) never re-creates it and
// every later new-style request is blackholed. Model: muxunsub_bug.cfg,
// muxunsub_hang_bug.cfg.
// ---------------------------------------------------------------------------

func TestREQ2RequestAfterReplySubUnsubscribe(t *testing.T) {
	harness.KnownBug(t, "REQ-2")
	for _, how := range []string{"Unsubscribe", "AutoUnsubscribe", "Drain"} {
		t.Run(how, func(t *testing.T) {
			s := harness.RunServer(t, false)
			nc := connect(t, s.URL())
			echo(t, nc, "svc")
			m, err := nc.Request("svc", []byte("one"), 2*time.Second)
			if err != nil {
				t.Fatalf("first request: %v", err)
			}
			// What a caller might do with "its" subscription.
			switch how {
			case "Unsubscribe":
				err = m.Sub.Unsubscribe()
			case "AutoUnsubscribe":
				err = m.Sub.AutoUnsubscribe(1)
			case "Drain":
				err = m.Sub.Drain()
			}
			if err != nil {
				t.Fatalf("%s: %v", how, err)
			}
			harness.Eventually(2*time.Second, func() bool { return !m.Sub.IsValid() })
			if _, err := nc.Request("svc", []byte("two"), 2*time.Second); err != nil {
				st := Peek(nc)
				t.Fatalf("request after %s on a reply's Sub: %v (respMux set=%v valid=%v)", how, err, st.HasMux, st.MuxValid)
			}
		})
	}
}

// With no deadline the same request never returns: RequestWithContext only
// comes back when the caller's context ends.
func TestREQ2RequestWithContextHangs(t *testing.T) {
	harness.KnownBug(t, "REQ-2")
	s := harness.RunServer(t, false)
	nc := connect(t, s.URL())
	echo(t, nc, "svc")
	m, err := nc.Request("svc", []byte("one"), 2*time.Second)
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	if err := m.Sub.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := nc.RequestWithContext(ctx, "svc", []byte("two")); err != nil {
		t.Fatalf("RequestWithContext: %v (the responder is up; only the deadline released the call)", err)
	}
}

// Knock-on effect in drainConnection (nats.go:6393-6420): it waits for the
// subscription count to fall to 1 because respMux != nil, which never
// happens once the mux is gone, so Drain always runs out DrainTimeout.
func TestREQ2DrainRunsOutTimeout(t *testing.T) {
	harness.KnownBug(t, "REQ-2")
	s := harness.RunServer(t, false)
	nc := connect(t, s.URL(), nats.DrainTimeout(3*time.Second))
	resp := connect(t, s.URL())
	echo(t, resp, "svc")
	m, err := nc.Request("svc", []byte("one"), 2*time.Second)
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	if err := m.Sub.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	start := time.Now()
	if err := nc.Drain(); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if !harness.Eventually(5*time.Second, nc.IsClosed) {
		t.Fatalf("drain did not close the connection")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Drain of an idle connection took %v (DrainTimeout 3s)", d)
	}
}

// ---------------------------------------------------------------------------
// REQ-3: respHandler's fallback (nats.go:4816-4824) hands a message whose
// subject lacks the mux prefix to whichever single request is registered,
// whoever it was meant for. Model: foreign_bug.cfg. Needs a server that
// delivers such a subject on the mux sid, so it is shown with the scripted
// server.
// ---------------------------------------------------------------------------

func TestREQ3FallbackHandsStaleReplyToOtherRequest(t *testing.T) {
	harness.KnownBug(t, "REQ-3")
	fs := newFakeServer(t)
	nc := connect(t, fs.URL())

	// Request A times out.
	if _, err := nc.Request("a", []byte("A"), 100*time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("request A: err = %v, want timeout", err)
	}
	sid, _ := fs.WaitSub("_INBOX.")
	pa := fs.WaitPub("a", 0)

	// Request B is in flight; A's reply shows up with a rewritten subject.
	type res struct {
		m   *nats.Msg
		err error
	}
	done := make(chan res, 1)
	go func() {
		m, err := nc.Request("b", []byte("B"), 2*time.Second)
		done <- res{m, err}
	}()
	pb := fs.WaitPub("b", 0)
	fs.Msg("rewritten."+pa.Reply[strings.LastIndexByte(pa.Reply, '.')+1:], sid, []byte("reply-for-A"))
	fs.Msg(pb.Reply, sid, []byte("reply-for-B"))
	r := <-done
	if r.err != nil {
		t.Fatalf("request B: %v", r.err)
	}
	if string(r.m.Data) != "reply-for-B" {
		t.Fatalf("request B returned %q (subject %q), a reply meant for request A", r.m.Data, r.m.Subject)
	}
}
