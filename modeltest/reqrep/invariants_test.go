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

// Deterministic checks of the RequestReply invariants that TLC proved for
// the model, run against the real client with the scripted server so that
// each environment action happens exactly when the model's trace says.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/modeltest/harness"
)

type result struct {
	m   *nats.Msg
	err error
}

func goRequest(nc *nats.Conn, subj string, data []byte, timeout time.Duration) <-chan result {
	ch := make(chan result, 1)
	go func() {
		m, err := nc.Request(subj, data, timeout)
		ch <- result{m, err}
	}()
	return ch
}

func await(t *testing.T, ch <-chan result, within time.Duration) result {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(within):
		t.Fatalf("request did not return within %v", within)
		return result{}
	}
}

// OneSendPerRequest / NoMisdelivery / NoLeak with duplicate replies, a
// reply for an unknown token and a late reply after a timeout (dup.cfg).
func TestDuplicateUnknownAndLateRepliesAreDropped(t *testing.T) {
	fs := newFakeServer(t)
	nc := connect(t, fs.URL())

	// A: times out; its reply arrives late.
	if _, err := nc.Request("a", []byte("A"), 50*time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("A: err = %v, want timeout", err)
	}
	sid, _ := fs.WaitSub("_INBOX.")
	pa := fs.WaitPub("a", 0)

	// B: in flight while A's late reply, a reply to a token nobody owns and
	// then B's reply twice arrive.
	rb := goRequest(nc, "b", []byte("B"), 5*time.Second)
	pb := fs.WaitPub("b", 0)
	fs.Msg(pa.Reply, sid, []byte("late-A"))
	fs.Msg(pa.Reply[:len(pa.Reply)-8]+"ZZZZZZZZ", sid, []byte("nobody"))
	fs.Msg(pb.Reply, sid, []byte("B-1"))
	fs.Msg(pb.Reply, sid, []byte("B-2"))
	r := await(t, rb, 5*time.Second)
	if r.err != nil || string(r.m.Data) != "B-1" {
		t.Fatalf("B: got %v / %v, want B-1", r.m, r.err)
	}
	// C: the duplicates above must not leak into the next request.
	rc := goRequest(nc, "c", []byte("C"), 5*time.Second)
	pc := fs.WaitPub("c", 0)
	fs.Msg(pc.Reply, sid, []byte("C-1"))
	r = await(t, rc, 5*time.Second)
	if r.err != nil || string(r.m.Data) != "C-1" {
		t.Fatalf("C: got %v / %v, want C-1", r.m, r.err)
	}
	if n := Peek(nc).RespMapLen; n != 0 {
		t.Fatalf("respMap holds %d entries with nothing in flight", n)
	}
}

// The no-responders status (nats.go:4909, context.go:80, 5842) for both
// styles, and the entry is removed.
func TestNoRespondersStatus(t *testing.T) {
	for _, old := range []bool{false, true} {
		t.Run(fmt.Sprintf("old=%v", old), func(t *testing.T) {
			fs := newFakeServer(t)
			opts := []nats.Option{}
			if old {
				opts = append(opts, nats.UseOldRequestStyle())
			}
			nc := connect(t, fs.URL(), opts...)
			rr := goRequest(nc, "nobody", nil, 5*time.Second)
			p := fs.WaitPub("nobody", 0)
			sid, _ := fs.WaitSub(p.Reply[:7])
			if !old {
				sid, _ = fs.WaitSub(p.Reply[:len(p.Reply)-8])
			}
			fs.NoResponders(p.Reply, sid)
			r := await(t, rr, 5*time.Second)
			if !errors.Is(r.err, nats.ErrNoResponders) {
				t.Fatalf("err = %v, want ErrNoResponders", r.err)
			}
			if n := Peek(nc).RespMapLen; n != 0 {
				t.Fatalf("respMap holds %d entries", n)
			}
		})
	}
}

// MuxCreatedOnce: many goroutines making the first request concurrently
// create exactly one mux subscription.
func TestConcurrentFirstRequestsCreateOneMux(t *testing.T) {
	fs := newFakeServer(t)
	nc := connect(t, fs.URL())
	const n = 64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _ = nc.Request("x", nil, 200*time.Millisecond)
		}()
	}
	close(start)
	wg.Wait()
	if !harness.Eventually(2*time.Second, func() bool { return fs.PubCount("x") == n }) {
		t.Fatalf("server saw %d of %d requests", fs.PubCount("x"), n)
	}
	if c := fs.SubCount("_INBOX."); c != 1 {
		t.Fatalf("%d mux subscriptions created, want 1", c)
	}
	if st := Peek(nc); !st.HasMux || !st.MuxValid || st.RespMapLen != 0 {
		t.Fatalf("after all timed out: %+v", st)
	}
}

// CloseReleasesWaiters: every in-flight request - new style with and
// without context, old style - returns ErrConnectionClosed promptly when
// the connection is closed (clearPendingRequestCalls nats.go:6190, the sync
// subs loop nats.go:6247).
func TestCloseReleasesInflightRequests(t *testing.T) {
	fs := newFakeServer(t)
	nc := connect(t, fs.URL())
	ncOld := connect(t, fs.URL(), nats.UseOldRequestStyle())
	const n = 8
	var chans []<-chan result
	for i := 0; i < n; i++ {
		chans = append(chans, goRequest(nc, "silent", nil, time.Minute))
		chans = append(chans, goRequest(ncOld, "silent", nil, time.Minute))
		ch := make(chan result, 1)
		go func() {
			m, err := nc.RequestWithContext(context.Background(), "silent", nil)
			ch <- result{m, err}
		}()
		chans = append(chans, ch)
	}
	if !harness.Eventually(5*time.Second, func() bool { return fs.PubCount("silent") == 3*n }) {
		t.Fatalf("server saw %d of %d requests", fs.PubCount("silent"), 3*n)
	}
	nc.Close()
	ncOld.Close()
	for i, ch := range chans {
		r := await(t, ch, 2*time.Second)
		if !errors.Is(r.err, nats.ErrConnectionClosed) {
			t.Fatalf("request %d: err = %v, want ErrConnectionClosed", i, r.err)
		}
	}
}

// Across a reconnect the mux is re-subscribed before buffered requests are
// flushed (doReconnect nats.go:3588-3591), including a mux first created
// while RECONNECTING (SUB suppressed, nats.go:5220).
func TestRequestsAcrossReconnect(t *testing.T) {
	s := harness.RunServer(t, false)
	p := harness.NewProxy(t, s.Addr())
	resp := connect(t, s.URL())
	echo(t, resp, "svc")
	reconnected := make(chan struct{}, 16)
	nc := connect(t, p.URL(), nats.ReconnectWait(20*time.Millisecond), nats.MaxReconnects(-1),
		nats.ReconnectHandler(func(*nats.Conn) { reconnected <- struct{}{} }))

	p.Block()
	if !harness.Eventually(5*time.Second, nc.IsReconnecting) {
		t.Fatalf("client did not notice the outage")
	}
	// First request ever: the mux is created while reconnecting.
	r := goRequest(nc, "svc", []byte("during"), 10*time.Second)
	time.Sleep(50 * time.Millisecond)
	p.Unblock()
	<-reconnected
	if got := await(t, r, 10*time.Second); got.err != nil || string(got.m.Data) != "during" {
		t.Fatalf("request issued while reconnecting: %v / %v", got.m, got.err)
	}
	for i := 0; i < 3; i++ {
		// A request published into the connection being cut is lost (no
		// retransmission); wait for the reconnect before asking.
		p.Cut()
		select {
		case <-reconnected:
		case <-time.After(10 * time.Second):
			t.Fatalf("no reconnect")
		}
		m, err := nc.Request("svc", []byte(fmt.Sprint(i)), 5*time.Second)
		if err != nil || string(m.Data) != fmt.Sprint(i) {
			t.Fatalf("request after reconnect %d: %v / %v", i, m, err)
		}
	}
	if st := Peek(nc); !st.MuxValid || st.RespMapLen != 0 {
		t.Fatalf("after reconnects: %+v", st)
	}
}
