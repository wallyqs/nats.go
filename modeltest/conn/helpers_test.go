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

// Package conn holds the tests derived from models/ConnLifecycle: the
// reproducers for its findings (bugs_test.go) and the zizek state machines
// whose rules are the model's actions and whose invariants are the model's
// invariants (machine_test.go).
package conn

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/modeltest/harness"
)

// ---------------------------------------------------------------------------
// faultDialer: a nats.CustomDialer whose connections can be told to fail a
// write part way (the model's WriteFails action), to hold a read that has
// already been taken off the socket (the model's "readLoop is preempted
// between Read and parse"), and to refuse new dials (server unreachable).
// ---------------------------------------------------------------------------

type faultDialer struct {
	mu      sync.Mutex
	conns   []*faultConn
	dials   atomic.Int64
	refuse  atomic.Bool
	wfault  *writeFault // armed write fault, consumed by the first match
	rhold   *readHold   // armed read hold, consumed by the first match
	address string
}

type writeFault struct {
	marker    string // fail the first write that contains this
	closeConn bool   // also close the socket (EPIPE-like) instead of leaving it open (timeout-like)
	fired     chan struct{}
	data      []byte // the write that failed
}

type readHold struct {
	conn    *faultConn // only reads on this socket (the newest one when armed)
	marker  string     // hold the first read whose data contains this
	held    chan struct{}
	release chan struct{}
}

func (d *faultDialer) Dial(network, address string) (net.Conn, error) {
	d.dials.Add(1)
	if d.refuse.Load() {
		return nil, errors.New("faultDialer: refused")
	}
	c, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	fc := &faultConn{Conn: c, d: d}
	d.mu.Lock()
	d.conns = append(d.conns, fc)
	d.mu.Unlock()
	return fc, nil
}

// armWrite makes the first subsequent write containing marker write half of
// its bytes and fail with os.ErrDeadlineExceeded (what timeoutWriter returns
// when FlusherTimeout expires part way through a write).
func (d *faultDialer) armWrite(marker string, closeConn bool) *writeFault {
	f := &writeFault{marker: marker, closeConn: closeConn, fired: make(chan struct{})}
	d.mu.Lock()
	d.wfault = f
	d.mu.Unlock()
	return f
}

// armRead makes the first subsequent read on the newest socket whose data
// contains marker block, with the data already taken off the socket, until
// release is closed. (Restricting it to that socket keeps it away from the
// handshake reads of later sockets, which run under the connection lock.)
func (d *faultDialer) armRead(marker string) *readHold {
	h := &readHold{marker: marker, held: make(chan struct{}), release: make(chan struct{})}
	d.mu.Lock()
	if len(d.conns) > 0 {
		h.conn = d.conns[len(d.conns)-1]
	}
	d.rhold = h
	d.mu.Unlock()
	return h
}

type faultConn struct {
	net.Conn
	d         *faultDialer
	written   []byte // every write attempted on this socket, failed ones included (d.mu)
	pongsRead int    // PONGs read from this socket (d.mu)
}

// pingsAnswered reports whether the newest socket has settled: every PING
// written on it has been answered by a PONG read from it, and on a socket
// that a reconnect opened (any but the first), doReconnect's closing Flush
// has been written too, so that there are at least two PINGs (the
// handshake's and that Flush's).
func (d *faultDialer) pingsAnswered() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.conns) == 0 {
		return true
	}
	c := d.conns[len(d.conns)-1]
	pings := bytes.Count(c.written, []byte("PING\r\n"))
	if len(d.conns) > 1 && pings < 2 {
		return false
	}
	return pings <= c.pongsRead
}

// pingFollows reports whether b was written (or attempted) on some socket
// and a PING was written on that same socket after it.
func (d *faultDialer) pingFollows(b []byte) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.conns {
		if i := bytes.Index(c.written, b); i >= 0 && bytes.Contains(c.written[i+len(b):], []byte("PING")) {
			return true
		}
	}
	return false
}

// pings counts the PINGs written (or attempted) on all sockets.
func (d *faultDialer) pings() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, c := range d.conns {
		n += bytes.Count(c.written, []byte("PING\r\n"))
	}
	return n
}

func (c *faultConn) Write(p []byte) (int, error) {
	c.d.mu.Lock()
	c.written = append(c.written, p...)
	f := c.d.wfault
	if f != nil && bytes.Contains(p, []byte(f.marker)) {
		c.d.wfault = nil
	} else {
		f = nil
	}
	c.d.mu.Unlock()
	if f == nil {
		return c.Conn.Write(p)
	}
	f.data = append([]byte(nil), p...)
	n, _ := c.Conn.Write(p[:len(p)/2])
	if f.closeConn {
		c.Conn.Close()
	}
	close(f.fired)
	return n, os.ErrDeadlineExceeded
}

func (c *faultConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.d.mu.Lock()
		c.pongsRead += bytes.Count(p[:n], []byte("PONG\r\n"))
		h := c.d.rhold
		if h != nil && h.conn == c && bytes.Contains(p[:n], []byte(h.marker)) {
			c.d.rhold = nil
		} else {
			h = nil
		}
		c.d.mu.Unlock()
		if h != nil {
			close(h.held)
			<-h.release
		}
	}
	return n, err
}

// ---------------------------------------------------------------------------
// fakeServer: a scripted NATS server on raw TCP. Each accepted connection is
// handed to the script with its index, so a test can make the first
// connection healthy and the later ones misbehave (withhold PONG, reply
// -ERR, stop reading, never close).
// ---------------------------------------------------------------------------

type fakeServer struct {
	t      testing.TB
	ln     net.Listener
	mu     sync.Mutex
	conns  []net.Conn
	n      atomic.Int64
	script func(i int, c *fakeConn)
	wg     sync.WaitGroup
}

type fakeConn struct {
	net.Conn
	r *bufio.Reader
}

const fakeInfo = `INFO {"server_id":"FAKE","version":"2.10.0","proto":1,"headers":true,"max_payload":1048576}` + "\r\n"

func newFakeServer(t testing.TB, script func(i int, c *fakeConn)) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake listen: %v", err)
	}
	fs := &fakeServer{t: t, ln: ln, script: script}
	go fs.accept()
	t.Cleanup(fs.Close)
	return fs
}

func (fs *fakeServer) URL() string { return "nats://" + fs.ln.Addr().String() }

func (fs *fakeServer) accept() {
	for {
		c, err := fs.ln.Accept()
		if err != nil {
			return
		}
		fs.mu.Lock()
		fs.conns = append(fs.conns, c)
		fs.mu.Unlock()
		i := int(fs.n.Add(1)) - 1
		fs.wg.Add(1)
		go func() {
			defer fs.wg.Done()
			fs.script(i, &fakeConn{Conn: c, r: bufio.NewReader(c)})
		}()
	}
}

func (fs *fakeServer) Close() {
	fs.ln.Close()
	fs.mu.Lock()
	for _, c := range fs.conns {
		c.Close()
	}
	fs.mu.Unlock()
	fs.wg.Wait()
}

// line reads one protocol line (without CRLF).
func (c *fakeConn) line() (string, error) {
	s, err := c.r.ReadString('\n')
	return strings.TrimRight(s, "\r\n"), err
}

// handshake sends INFO, reads CONNECT and PING, and returns the error, if
// any. It does not answer the PING.
func (c *fakeConn) handshake() error {
	if _, err := c.Write([]byte(fakeInfo)); err != nil {
		return err
	}
	for _, want := range []string{"CONNECT", "PING"} {
		l, err := c.line()
		if err != nil {
			return err
		}
		if !strings.HasPrefix(l, want) {
			return errors.New("fake: expected " + want + ", got " + l)
		}
	}
	return nil
}

// serve answers PING with PONG and discards everything else until the
// connection fails.
func (c *fakeConn) serve() {
	for {
		l, err := c.line()
		if err != nil {
			return
		}
		if l == "PING" {
			c.Write([]byte("PONG\r\n"))
		}
	}
}

// waitEOF reports whether the peer closed the connection within d.
func (c *fakeConn) waitEOF(d time.Duration) bool {
	c.SetReadDeadline(time.Now().Add(d))
	for {
		if _, err := c.r.ReadByte(); err != nil {
			var ne net.Error
			return !(errors.As(err, &ne) && ne.Timeout())
		}
	}
}

// ---------------------------------------------------------------------------
// misc
// ---------------------------------------------------------------------------

// serverSubs counts the subscriptions the server holds on subject, over all
// client connections.
func serverSubs(t testing.TB, s *harness.Server, subject string) int {
	t.Helper()
	cz, err := s.Inner().Connz(&server.ConnzOptions{SubscriptionsDetail: true, Limit: 1024})
	if err != nil {
		t.Fatalf("connz: %v", err)
	}
	n := 0
	for _, c := range cz.Conns {
		for _, sd := range c.SubsDetail {
			if sd.Subject == subject {
				n++
			}
		}
	}
	return n
}

func waitStatus(t testing.TB, nc *nats.Conn, st nats.Status, d time.Duration) {
	t.Helper()
	if !harness.Eventually(d, func() bool { return nc.Status() == st }) {
		t.Fatalf("status never became %v (is %v)", st, nc.Status())
	}
}

func quietErrs(*nats.Conn, *nats.Subscription, error) {}

func connect(t testing.TB, url string, opts ...nats.Option) *nats.Conn {
	t.Helper()
	opts = append([]nats.Option{nats.ErrorHandler(quietErrs)}, opts...)
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// cbLog records the async connection callbacks in the order the dispatcher
// runs them.
type cbLog struct {
	mu  sync.Mutex
	evs []string
}

func (l *cbLog) add(s string) {
	l.mu.Lock()
	l.evs = append(l.evs, s)
	l.mu.Unlock()
}

func (l *cbLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.evs...)
}

func (l *cbLog) count(s string) int {
	n := 0
	for _, e := range l.get() {
		if e == s {
			n++
		}
	}
	return n
}

func (l *cbLog) opts() []nats.Option {
	return []nats.Option{
		nats.DisconnectErrHandler(func(*nats.Conn, error) { l.add("disconnected") }),
		nats.ReconnectHandler(func(*nats.Conn) { l.add("reconnected") }),
		nats.ClosedHandler(func(*nats.Conn) { l.add("closed") }),
	}
}
