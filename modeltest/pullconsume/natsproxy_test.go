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

package pullconsume

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// This file holds the environment side of the model for the Go tests: a
// TCP proxy that understands the NATS protocol well enough to see each
// frame, so a test can play the TLA+ model's environment actions on a real
// nats-server: drop idle heartbeats (a missed heartbeat), hold a 408 until
// the client is in a chosen state (a late expiry), cut the connection
// (disconnect/reconnect), and observe the pull requests the client sends
// (batch sizes on the wire).

// Frame is one protocol message seen by the proxy.
type Frame struct {
	ToClient bool   // server -> client when true
	Op       string // MSG, HMSG, PUB, HPUB, SUB, UNSUB, PING, PONG, INFO, CONNECT, +OK, -ERR
	Subject  string
	Reply    string
	Header   []byte // HMSG/HPUB headers
	Payload  []byte
	raw      []byte
}

// Status returns the status code of a header-only control message (e.g.
// "100" for an idle heartbeat, "408" for a request timeout), or "".
func (f *Frame) Status() string {
	if len(f.Header) == 0 || !bytes.HasPrefix(f.Header, []byte("NATS/1.0")) {
		return ""
	}
	line, _, _ := bytes.Cut(f.Header, []byte("\r\n"))
	fields := strings.Fields(string(line))
	if len(fields) < 2 {
		return ""
	}
	return fields[1]
}

// PullBatch returns the batch of a JetStream pull request frame and true,
// or 0 and false if the frame is not a pull request.
func (f *Frame) PullBatch() (int, bool) {
	if f.ToClient || (f.Op != "PUB" && f.Op != "HPUB") ||
		!strings.HasPrefix(f.Subject, "$JS.API.CONSUMER.MSG.NEXT.") {
		return 0, false
	}
	var req struct {
		Batch int `json:"batch"`
	}
	if err := json.Unmarshal(f.Payload, &req); err != nil {
		return 0, false
	}
	return req.Batch, true
}

// Verdict tells the proxy what to do with a frame.
type Verdict int

const (
	Pass Verdict = iota
	Drop
	Hold // queue it until Release
)

// NATSProxy forwards between clients and a nats-server, frame by frame.
type NATSProxy struct {
	t       testing.TB
	ln      net.Listener
	backend string

	mu     sync.Mutex
	filter func(*Frame) Verdict
	frames []*Frame // every frame seen, in order
	held   []heldFrame
	pairs  map[net.Conn]net.Conn
	closed bool
}

type heldFrame struct {
	dst net.Conn
	mu  *sync.Mutex
	f   *Frame
}

// NewNATSProxy starts a proxy in front of backend (host:port).
func NewNATSProxy(t testing.TB, backend string) *NATSProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &NATSProxy{t: t, ln: ln, backend: backend, pairs: map[net.Conn]net.Conn{}}
	go p.accept()
	t.Cleanup(p.Close)
	return p
}

// URL is the URL clients dial.
func (p *NATSProxy) URL() string { return "nats://" + p.ln.Addr().String() }

// SetFilter installs the frame filter (nil passes everything). The filter
// runs on the proxy's goroutines, with the proxy lock held: it must not
// call back into the proxy.
func (p *NATSProxy) SetFilter(f func(*Frame) Verdict) {
	p.mu.Lock()
	p.filter = f
	p.mu.Unlock()
}

// Frames returns a snapshot of every frame seen so far that matches pred.
func (p *NATSProxy) Frames(pred func(*Frame) bool) []*Frame {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []*Frame
	for _, f := range p.frames {
		if pred == nil || pred(f) {
			out = append(out, f)
		}
	}
	return out
}

// PullBatches returns the batch of every pull request sent so far.
func (p *NATSProxy) PullBatches() []int {
	var out []int
	for _, f := range p.Frames(nil) {
		if b, ok := f.PullBatch(); ok {
			out = append(out, b)
		}
	}
	return out
}

// Held returns how many frames are being held.
func (p *NATSProxy) Held() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.held)
}

// Release forwards every held frame, in order, to its destination.
func (p *NATSProxy) Release() {
	p.mu.Lock()
	held := p.held
	p.held = nil
	p.mu.Unlock()
	for _, h := range held {
		h.mu.Lock()
		_, _ = h.dst.Write(h.f.raw)
		h.mu.Unlock()
	}
}

// Cut closes every live connection; new connections are accepted, so a
// client with reconnect enabled comes straight back. Held frames are lost.
func (p *NATSProxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.held = nil
	for c, b := range p.pairs {
		c.Close()
		b.Close()
		delete(p.pairs, c)
	}
}

// Close shuts the proxy down.
func (p *NATSProxy) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.ln.Close()
	p.Cut()
}

func (p *NATSProxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		b, err := net.Dial("tcp", p.backend)
		if err != nil {
			c.Close()
			continue
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			c.Close()
			b.Close()
			return
		}
		p.pairs[c] = b
		p.mu.Unlock()
		var cmu, bmu sync.Mutex // serialize writes per destination
		go p.pump(b, &bmu, c, false)
		go p.pump(c, &cmu, b, true)
	}
}

// pump reads frames from src and forwards them to dst.
func (p *NATSProxy) pump(dst net.Conn, dmu *sync.Mutex, src net.Conn, toClient bool) {
	defer func() {
		src.Close()
		dst.Close()
	}()
	r := bufio.NewReaderSize(src, 64*1024)
	for {
		f, err := readFrame(r, toClient)
		if err != nil {
			return
		}
		p.mu.Lock()
		p.frames = append(p.frames, f)
		v := Pass
		if p.filter != nil {
			v = p.filter(f)
		}
		if v == Hold {
			p.held = append(p.held, heldFrame{dst: dst, mu: dmu, f: f})
		}
		p.mu.Unlock()
		if v != Pass {
			continue
		}
		dmu.Lock()
		_, err = dst.Write(f.raw)
		dmu.Unlock()
		if err != nil {
			return
		}
	}
}

func readFrame(r *bufio.Reader, toClient bool) (*Frame, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	raw := append([]byte(nil), line...)
	fields := strings.Fields(strings.TrimRight(string(line), "\r\n"))
	if len(fields) == 0 {
		return &Frame{ToClient: toClient, raw: raw}, nil
	}
	f := &Frame{ToClient: toClient, Op: strings.ToUpper(fields[0])}
	var hdrLen, totLen int
	switch f.Op {
	case "MSG": // MSG <subject> <sid> [reply] <size>
		f.Subject = fields[1]
		if len(fields) == 5 {
			f.Reply = fields[3]
		}
		totLen, err = strconv.Atoi(fields[len(fields)-1])
	case "HMSG": // HMSG <subject> <sid> [reply] <hdr> <total>
		f.Subject = fields[1]
		if len(fields) == 6 {
			f.Reply = fields[3]
		}
		hdrLen, err = strconv.Atoi(fields[len(fields)-2])
		if err == nil {
			totLen, err = strconv.Atoi(fields[len(fields)-1])
		}
	case "PUB": // PUB <subject> [reply] <size>
		f.Subject = fields[1]
		if len(fields) == 4 {
			f.Reply = fields[2]
		}
		totLen, err = strconv.Atoi(fields[len(fields)-1])
	case "HPUB": // HPUB <subject> [reply] <hdr> <total>
		f.Subject = fields[1]
		if len(fields) == 5 {
			f.Reply = fields[2]
		}
		hdrLen, err = strconv.Atoi(fields[len(fields)-2])
		if err == nil {
			totLen, err = strconv.Atoi(fields[len(fields)-1])
		}
	default:
		f.raw = raw
		return f, nil
	}
	if err != nil {
		return nil, fmt.Errorf("bad %s line %q: %w", f.Op, line, err)
	}
	body := make([]byte, totLen+2)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	f.Header = body[:hdrLen]
	f.Payload = body[hdrLen:totLen]
	f.raw = append(raw, body...)
	return f, nil
}

// goroutinesAt counts the goroutines whose stack contains every pattern
// (typically "jetstream/pull.go:<line>" of a blocking statement plus the
// blocking primitive, e.g. "sync.(*Mutex).Lock" or "chansend").
func goroutinesAt(patterns ...string) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		all := true
		for _, pat := range patterns {
			if !strings.Contains(g, pat) {
				all = false
				break
			}
		}
		if all {
			count++
		}
	}
	return count
}

// waitGoroutineAt waits until some goroutine's stack matches the patterns.
func waitGoroutineAt(t testing.TB, timeout time.Duration, patterns ...string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for goroutinesAt(patterns...) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("no goroutine reached %v within %v", patterns, timeout)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Stack patterns of two of Consume's goroutines: the message handler run by
// nats.go (the first closure in Consume) and the status goroutine (the
// third). They name functions, not lines, so they survive edits to pull.go.
const (
	consumeHandler  = "jetstream.(*pullConsumer).Consume.func1("
	statusGoroutine = "jetstream.(*pullConsumer).Consume.func3("
)

// consumeGoroutines counts goroutines running jetstream pull consumer code:
// the handler (inside waitForMsgs), the status goroutine, pullMessages and
// fired heartbeat timers.
func consumeGoroutines() int {
	return goroutinesAt("nats.go/jetstream.(*pullConsumer).Consume") +
		goroutinesAt("nats.go/jetstream.(*pullSubscription).pullMessages") +
		goroutinesAt("nats.go/jetstream.(*pullSubscription).scheduleHeartbeatCheck") +
		goroutinesAt("nats.go/jetstream.(*pullConsumer).Messages")
}

// chanLen returns len() of the channel in the named field of the struct
// that v points to. It only reads the length (reflect allows that on
// unexported fields); tests use it to wait for a settled state, e.g. for
// the status goroutine to have queued its notifications.
func chanLen(v any, field string) int {
	return reflect.ValueOf(v).Elem().FieldByName(field).Len()
}
