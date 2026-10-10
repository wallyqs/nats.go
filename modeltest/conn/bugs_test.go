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

package conn

// Reproducers for the findings of models/ConnLifecycle (see its README for
// the TLC traces). Each test asserts the correct behavior, so it fails on the
// current code; harness.KnownBug skips it unless MODELTEST_KNOWN_BUGS=1.

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/modeltest/harness"
)

// CONN-1: Conn.sendStatusEvent's "is the channel closed?" probe is a receive
// on the listener channel. When the channel holds an undelivered event, the
// probe consumes it and unregisters the listener (nats.go:6833-6851).
func TestConn1StatusListenerLosesEvents(t *testing.T) {
	s := harness.RunServer(t, false)
	p := harness.NewProxy(t, s.Addr())
	reconnected := make(chan struct{}, 4)
	nc := connect(t, p.URL(), nats.ReconnectWait(20*time.Millisecond),
		nats.ReconnectHandler(func(*nats.Conn) { reconnected <- struct{}{} }))
	ch := nc.StatusChanged() // default: CONNECTED, RECONNECTING, DISCONNECTED, CLOSED

	p.Cut()
	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("no reconnect")
	}
	// The listener read nothing yet; its channel has room for 10 events.
	var got []nats.Status
	for len(got) < 2 {
		select {
		case st := <-ch:
			got = append(got, st)
		case <-time.After(time.Second):
			t.Fatalf("listener got %v, want [RECONNECTING CONNECTED]", got)
		}
	}
	if got[0] != nats.RECONNECTING || got[1] != nats.CONNECTED {
		t.Fatalf("listener got %v, want [RECONNECTING CONNECTED]", got)
	}
}

// CONN-2: RemoveStatusListener closes the channel only when it is empty;
// otherwise it receives one event and leaves the channel open
// (nats.go:6799-6806), contradicting its doc comment.
func TestConn2RemoveStatusListenerDoesNotClose(t *testing.T) {
	s := harness.RunServer(t, false)
	p := harness.NewProxy(t, s.Addr())
	nc := connect(t, p.URL(), nats.ReconnectWait(20*time.Millisecond))
	ch := nc.StatusChanged(nats.RECONNECTING)
	p.Block()
	waitStatus(t, nc, nats.RECONNECTING, 5*time.Second)
	nc.RemoveStatusListener(ch)
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RemoveStatusListener left the channel open: a range over it never ends")
	}
}

// CONN-3: a subscription drained while the connection is reconnecting (or
// that is still draining when a reconnect happens) is re-sent by
// resendSubscriptions (nats.go:6141) and later removed client side by
// checkDrained (nats.go:5503) without an UNSUB: the server keeps the interest
// for the life of the connection.
func TestConn3DrainDuringReconnectLeaksServerInterest(t *testing.T) {
	s := harness.RunServer(t, false)
	p := harness.NewProxy(t, s.Addr())
	nc := connect(t, p.URL(), nats.ReconnectWait(20*time.Millisecond))
	sub, err := nc.SubscribeSync("conn3")
	if err != nil {
		t.Fatal(err)
	}
	nc.Flush()
	if n := serverSubs(t, s, "conn3"); n != 1 {
		t.Fatalf("server has %d subs, want 1", n)
	}
	p.Block()
	waitStatus(t, nc, nats.RECONNECTING, 5*time.Second)
	if err := sub.Drain(); err != nil {
		t.Fatal(err)
	}
	p.Unblock()
	waitStatus(t, nc, nats.CONNECTED, 5*time.Second)
	if !harness.Eventually(5*time.Second, func() bool { return !sub.IsValid() }) {
		t.Fatal("drain never completed")
	}
	nc.Flush()
	if !harness.Eventually(2*time.Second, func() bool { return serverSubs(t, s, "conn3") == 0 }) {
		t.Fatalf("subscription drained and removed client side, but the server still holds %d subscription(s) on it", serverSubs(t, s, "conn3"))
	}
}

// CONN-4 (a): AutoUnsubscribe(max) sends "UNSUB sid max" with max counted
// over the whole subscription (nats.go:5555-5559), but after a reconnect the
// server counts only the messages it sent since resendSubscriptions re-sent
// the SUB. Messages delivered before the reconnect are counted twice by the
// client and not at all by the server: the client removes the subscription
// after max, the server keeps it for that many more messages.
func TestConn4AutoUnsubscribeAfterReconnect(t *testing.T) {
	s := harness.RunServer(t, false)
	p := harness.NewProxy(t, s.Addr())
	pub := connect(t, s.URL())
	nc := connect(t, p.URL(), nats.ReconnectWait(20*time.Millisecond))
	sub, err := nc.SubscribeSync("conn4a")
	if err != nil {
		t.Fatal(err)
	}
	nc.Flush()
	for range 3 {
		pub.Publish("conn4a", nil)
	}
	pub.Flush()
	for range 3 {
		if _, err := sub.NextMsg(time.Second); err != nil {
			t.Fatal(err)
		}
	}
	p.Cut()
	waitStatus(t, nc, nats.CONNECTED, 5*time.Second)
	if !harness.Eventually(5*time.Second, func() bool { return nc.Stats().Reconnects == 1 }) {
		t.Fatal("no reconnect")
	}
	nc.Flush()
	if err := sub.AutoUnsubscribe(5); err != nil {
		t.Fatal(err)
	}
	nc.Flush()
	for range 2 {
		pub.Publish("conn4a", nil)
	}
	pub.Flush()
	for range 2 {
		if _, err := sub.NextMsg(time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if sub.IsValid() {
		t.Fatal("subscription still valid after 5 messages")
	}
	nc.Flush()
	if n := serverSubs(t, s, "conn4a"); n != 0 {
		t.Fatalf("client removed the AutoUnsubscribe(5) subscription after 5 messages, but the server still holds %d subscription(s) on it", n)
	}
}

// CONN-4 (b): on reconnect resendSubscriptions re-arms an AutoUnsubscribe(max)
// with max - delivered (nats.go:6156), but delivered does not count messages
// already received and still pending in the client. The client removes the
// subscription after max messages; the server keeps it for `pending` more.
func TestConn4AutoUnsubscribeResendIgnoresPending(t *testing.T) {
	s := harness.RunServer(t, false)
	p := harness.NewProxy(t, s.Addr())
	pub := connect(t, s.URL())
	nc := connect(t, p.URL(), nats.ReconnectWait(20*time.Millisecond))
	gate := make(chan struct{})
	var delivered atomic.Int64
	sub, err := nc.Subscribe("conn4", func(*nats.Msg) {
		delivered.Add(1)
		<-gate
	})
	if err != nil {
		t.Fatal(err)
	}
	sub.AutoUnsubscribe(5)
	nc.Flush()
	for range 3 {
		pub.Publish("conn4", nil)
	}
	pub.Flush()
	// One message in the callback, two more pending in the client (Pending
	// counts the one in the callback until it returns).
	if !harness.Eventually(2*time.Second, func() bool { n, _, _ := sub.Pending(); return n == 3 && delivered.Load() == 1 }) {
		t.Fatal("messages did not arrive")
	}
	p.Cut()
	// Wait for the reconnect itself: right after the cut the client may not
	// have noticed it yet and still report CONNECTED.
	if !harness.Eventually(5*time.Second, func() bool { return nc.Stats().Reconnects == 1 }) {
		t.Fatal("no reconnect")
	}
	waitStatus(t, nc, nats.CONNECTED, 5*time.Second)
	nc.Flush()
	close(gate)
	for range 2 {
		pub.Publish("conn4", nil)
	}
	pub.Flush()
	if !harness.Eventually(2*time.Second, func() bool { return !sub.IsValid() }) {
		t.Fatalf("sub still valid after %d deliveries", delivered.Load())
	}
	if delivered.Load() != 5 {
		t.Fatalf("delivered %d, want 5", delivered.Load())
	}
	nc.Flush()
	if n := serverSubs(t, s, "conn4"); n != 0 {
		t.Fatalf("client removed the AutoUnsubscribe(5) subscription after 5 messages, but the server still holds %d subscription(s) on it", n)
	}
}

// CONN-5a: when the flush of the reconnect pending buffer fails
// (nats.go:3591), doReconnect neither closes the new socket nor waits for it
// to die; the next iteration blocks in waitForExits until the server drops
// the connection. Here the write fails part way like a FlusherTimeout expiry
// on a slow server, and the client never retries.
func TestConn5PendingFlushFailureStallsReconnect(t *testing.T) {
	harness.KnownBug(t, "CONN-5")
	s := harness.RunServer(t, false)
	p := harness.NewProxy(t, s.Addr())
	d := &faultDialer{}
	nc := connect(t, p.URL(), nats.SetCustomDialer(d), nats.ReconnectWait(20*time.Millisecond))
	p.Block()
	waitStatus(t, nc, nats.RECONNECTING, 5*time.Second)
	if err := nc.Publish("conn5", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	f := d.armWrite("PUB conn5", false)
	p.Unblock()
	select {
	case <-f.fired:
	case <-time.After(5 * time.Second):
		t.Fatal("pending flush was never attempted")
	}
	dials := d.dials.Load()
	if !harness.Eventually(3*time.Second, func() bool { return nc.Status() == nats.CONNECTED }) {
		t.Fatalf("after the pending buffer flush failed the client is stuck in %v; dials since: %d", nc.Status(), d.dials.Load()-dials)
	}
}

// CONN-5b: the same failure path keeps the pending flush waiters (nc.pongs)
// whose PINGs were in the discarded pending buffer. The next PONG on the next
// connection completes the oldest of them: Flush returns nil although the
// message published before it never reached the server.
func TestConn5FlushSucceedsForLostMessage(t *testing.T) {
	harness.KnownBug(t, "CONN-5")
	s := harness.RunServer(t, false)
	p := harness.NewProxy(t, s.Addr())
	sc := connect(t, s.URL())
	sub, _ := sc.SubscribeSync("conn5b")
	sc.Flush()
	d := &faultDialer{}
	nc := connect(t, p.URL(), nats.SetCustomDialer(d), nats.ReconnectWait(20*time.Millisecond))
	p.Block()
	waitStatus(t, nc, nats.RECONNECTING, 5*time.Second)
	if err := nc.Publish("conn5b", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	flushErr := make(chan error, 1)
	go func() { flushErr <- nc.FlushTimeout(5 * time.Second) }()
	time.Sleep(50 * time.Millisecond) // let Flush queue its PING behind the PUB
	d.armWrite("PUB conn5b", true)
	p.Unblock()
	err := <-flushErr
	if err == nil {
		if _, merr := sub.NextMsg(time.Second); merr != nil {
			t.Fatalf("Flush returned nil, but the message published before it was never delivered (%v)", merr)
		}
	}
}

// CONN-6: after clearPendingFlushCalls (ForceReconnect, processOpErr from the
// flusher / ping timer / an -ERR mid-buffer) the old readLoop may still be
// parsing bytes it read before the socket was closed. A PONG in them is
// matched by processPong (nats.go:4272) against the *new* pongs list, so a
// Flush issued during the reconnect returns nil without any round trip.
func TestConn6StalePongCompletesNewFlush(t *testing.T) {
	harness.KnownBug(t, "CONN-6")
	s := harness.RunServer(t, false)
	p := harness.NewProxy(t, s.Addr())
	d := &faultDialer{}
	nc := connect(t, p.URL(), nats.SetCustomDialer(d), nats.ReconnectWait(20*time.Millisecond))
	h := d.armRead("PONG")
	first := make(chan error, 1)
	go func() { first <- nc.Flush() }()
	<-h.held // readLoop has the PONG in hand
	if err := nc.ForceReconnect(); err != nil {
		t.Fatal(err)
	}
	<-first // released by clearPendingFlushCalls
	p.Block()
	second := make(chan error, 1)
	go func() { second <- nc.FlushTimeout(2 * time.Second) }()
	time.Sleep(50 * time.Millisecond) // second Flush is queued
	close(h.release)
	select {
	case err := <-second:
		if err == nil {
			t.Fatalf("Flush returned nil while the server is unreachable (status %v): it was completed by the old connection's PONG", nc.Status())
		}
	case <-time.After(3 * time.Second):
	}
}

// CONN-7: ForceReconnect while RECONNECTING closes nc.rqch, but doReconnect
// selects on the local copy taken at entry (nats.go:3393), which stays closed:
// every later back-off sleep of that reconnect episode returns immediately and
// MaxReconnects is exhausted in a tight loop.
func TestConn7ForceReconnectDisablesBackoff(t *testing.T) {
	harness.KnownBug(t, "CONN-7")
	s := harness.RunServer(t, false)
	var attempts atomic.Int64
	nc := connect(t, s.URL(),
		nats.ReconnectWait(250*time.Millisecond),
		nats.MaxReconnects(8),
		nats.ReconnectErrHandler(func(*nats.Conn, error) { attempts.Add(1) }))
	s.Shutdown()
	if !harness.Eventually(5*time.Second, func() bool { return attempts.Load() >= 1 }) {
		t.Fatal("no reconnect attempt")
	}
	start := time.Now()
	before := attempts.Load()
	if err := nc.ForceReconnect(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond)
	// One immediate attempt is what ForceReconnect asks for; after that the
	// 250ms back-off applies, so at most ~3 more attempts fit in 600ms.
	if n := attempts.Load() - before; n > 4 || nc.IsClosed() {
		t.Fatalf("%d reconnect attempts in %v after ForceReconnect (closed=%v): back-off ignored", n, time.Since(start), nc.IsClosed())
	}
}

// firstRetryFast makes the first reconnect attempt immediate and every later
// one wait 5s, so a test can look at the state right after one failed attempt.
var firstRetryFast = nats.CustomReconnectDelay(func(attempts int) time.Duration {
	if attempts <= 1 {
		return 10 * time.Millisecond
	}
	return 5 * time.Second
})

// CONN-8: when the handshake on a freshly dialed reconnect socket fails,
// doReconnect continues without closing it (nats.go:3569-3576); it stays open
// until the next dial overwrites nc.conn and the garbage collector finalizes
// it.
func TestConn8FailedReconnectHandshakeLeaksSocket(t *testing.T) {
	harness.KnownBug(t, "CONN-8")
	eof := make(chan bool, 1)
	fs := newFakeServer(t, func(i int, c *fakeConn) {
		if err := c.handshake(); err != nil {
			return
		}
		if i == 0 {
			c.Write([]byte("PONG\r\n"))
			time.Sleep(50 * time.Millisecond)
			c.Close() // trigger the reconnect
			return
		}
		if i == 1 {
			c.Write([]byte("-ERR 'Unexpected Failure'\r\n"))
			eof <- c.waitEOF(time.Second)
		}
		c.serve()
	})
	connect(t, fs.URL(), firstRetryFast)
	select {
	case closed := <-eof:
		if !closed {
			t.Fatal("client left the socket of its failed reconnect handshake open")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no reconnect attempt")
	}
}

// CONN-9: the same leftover nc.conn makes a Close during the reconnect back-off
// deliver a second DisconnectedErrCB (close() keys it on nc.conn != nil,
// nats.go:6287), after the one doReconnect already delivered.
func TestConn9DoubleDisconnectCallback(t *testing.T) {
	harness.KnownBug(t, "CONN-9")
	attempted := make(chan struct{}, 1)
	fs := newFakeServer(t, func(i int, c *fakeConn) {
		if err := c.handshake(); err != nil {
			return
		}
		if i == 0 {
			c.Write([]byte("PONG\r\n"))
			time.Sleep(50 * time.Millisecond)
			c.Close()
			return
		}
		c.Write([]byte("-ERR 'Unexpected Failure'\r\n"))
		attempted <- struct{}{}
		c.serve()
	})
	var log cbLog
	nc := connect(t, fs.URL(), append(log.opts(), firstRetryFast)...)
	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		t.Fatal("no reconnect attempt")
	}
	time.Sleep(50 * time.Millisecond)
	nc.Close()
	if !harness.Eventually(2*time.Second, func() bool { return log.count("closed") == 1 }) {
		t.Fatalf("no closed callback: %v", log.get())
	}
	if n := log.count("disconnected"); n != 1 {
		t.Fatalf("one disconnect, %d DisconnectedErrCB calls: %v", n, log.get())
	}
}

// CONN-10: a sync subscription drained and then unsubscribed has its closed
// handler called twice: once by Unsubscribe's removeSub and again by
// checkDrained, which calls removeSub even when the subscription is already
// closed (nats.go:5503-5506).
func TestConn10ClosedHandlerCalledTwice(t *testing.T) {
	harness.KnownBug(t, "CONN-10")
	s := harness.RunServer(t, false)
	nc := connect(t, s.URL())
	sub, err := nc.SubscribeSync("conn10")
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	sub.SetClosedHandler(func(string) { calls.Add(1) })
	if err := sub.Drain(); err != nil {
		t.Fatal(err)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Fatalf("closed handler called %d times", n)
	}
}

// CONN-11: when Close runs after processOpErr has started a reconnect but
// before doReconnect has queued DisconnectedErrCB (doReconnect first waits for
// the readLoop to exit, nats.go:3359), close() sees nc.conn == nil and queues
// only ClosedCB and the dispatcher's close marker; the DisconnectedErrCB that
// doReconnect queues afterwards is never run. Here the ping timer declares the
// connection stale while the readLoop is still busy with bytes it has read.
func TestConn11CloseLosesDisconnectCallback(t *testing.T) {
	harness.KnownBug(t, "CONN-11")
	s := harness.RunServer(t, false)
	d := &faultDialer{}
	var log cbLog
	nc := connect(t, s.URL(), append(log.opts(),
		nats.SetCustomDialer(d),
		nats.PingInterval(50*time.Millisecond),
		nats.MaxPingsOutstanding(1))...)
	h := d.armRead("PONG")
	go nc.FlushTimeout(time.Second)
	<-h.held // the readLoop holds a PONG it has not parsed: pout is not reset
	waitStatus(t, nc, nats.RECONNECTING, 2*time.Second)
	nc.Close()
	close(h.release)
	if !harness.Eventually(2*time.Second, func() bool { return log.count("closed") == 1 }) {
		t.Fatalf("no ClosedCB: %v", log.get())
	}
	time.Sleep(100 * time.Millisecond)
	if log.count("disconnected") != 1 {
		t.Fatalf("connection was lost (stale) and then closed, callbacks: %v", log.get())
	}
}
