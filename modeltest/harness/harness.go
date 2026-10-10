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

// Package harness holds the shared plumbing for the model-derived tests:
// an embedded nats-server that can be restarted on the same port, and a
// TCP proxy that lets a test cut, stall, and restore the client's
// connection without touching the server. Each TLA+ model under ../models
// has a sibling test package here that drives nats.go through the
// model's actions with zizek and checks the model's invariants against
// the real implementation.
package harness

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// Server is an embedded nats-server bound to a fixed port so that it can be
// shut down and restarted in place (clients reconnect to the same URL).
type Server struct {
	t         testing.TB
	opts      server.Options
	srv       *server.Server
	JetStream bool
}

// RunServer starts an embedded server on a random free port. JetStream is
// enabled when js is true, with its store under t.TempDir() so that it
// survives a Restart.
func RunServer(t testing.TB, js bool) *Server {
	t.Helper()
	opts := server.Options{
		Host:   "127.0.0.1",
		Port:   freePort(t),
		NoLog:  true,
		NoSigs: true,
	}
	if js {
		opts.JetStream = true
		opts.StoreDir = t.TempDir()
	}
	s := &Server{t: t, opts: opts, JetStream: js}
	s.start()
	t.Cleanup(s.Shutdown)
	return s
}

func (s *Server) start() {
	s.t.Helper()
	opts := s.opts
	srv, err := server.NewServer(&opts)
	if err != nil {
		s.t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		s.t.Fatalf("server not ready")
	}
	if s.JetStream {
		deadline := time.Now().Add(10 * time.Second)
		for !srv.JetStreamEnabled() && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
	}
	s.srv = srv
}

// URL is the client URL; stable across restarts.
func (s *Server) URL() string { return fmt.Sprintf("nats://127.0.0.1:%d", s.opts.Port) }

// Addr is host:port; stable across restarts.
func (s *Server) Addr() string { return fmt.Sprintf("127.0.0.1:%d", s.opts.Port) }

// Inner exposes the running *server.Server (nil while stopped).
func (s *Server) Inner() *server.Server { return s.srv }

// Shutdown stops the server; it is a no-op when already stopped.
func (s *Server) Shutdown() {
	if s.srv != nil {
		s.srv.Shutdown()
		s.srv.WaitForShutdown()
		s.srv = nil
	}
}

// Restart shuts the server down and starts it again on the same port and
// store directory.
func (s *Server) Restart() {
	s.Shutdown()
	s.start()
}

// Running reports whether the server is up.
func (s *Server) Running() bool { return s.srv != nil }

func freePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// Proxy is a TCP forwarder between clients and a backend address. Tests
// connect clients to Proxy.URL() and use Cut / Block / Unblock to inject the
// network faults that the TLA+ models treat as environment actions.
type Proxy struct {
	t       testing.TB
	ln      net.Listener
	backend string

	mu      sync.Mutex
	conns   map[net.Conn]net.Conn
	blocked bool
	closed  bool
}

// NewProxy listens on a random local port and forwards to backend.
func NewProxy(t testing.TB, backend string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &Proxy{t: t, ln: ln, backend: backend, conns: map[net.Conn]net.Conn{}}
	go p.accept()
	t.Cleanup(p.Close)
	return p
}

// URL is the URL clients should dial.
func (p *Proxy) URL() string { return "nats://" + p.ln.Addr().String() }

func (p *Proxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.blocked || p.closed {
			p.mu.Unlock()
			c.Close()
			continue
		}
		p.mu.Unlock()
		b, err := net.Dial("tcp", p.backend)
		if err != nil {
			c.Close()
			continue
		}
		p.mu.Lock()
		if p.blocked || p.closed {
			p.mu.Unlock()
			c.Close()
			b.Close()
			continue
		}
		p.conns[c] = b
		p.mu.Unlock()
		go p.pipe(c, b)
		go p.pipe(b, c)
	}
}

func (p *Proxy) pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	dst.Close()
	src.Close()
}

// Cut closes every live client connection. New connections are still
// accepted, so a client with reconnect enabled comes straight back.
func (p *Proxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c, b := range p.conns {
		c.Close()
		b.Close()
		delete(p.conns, c)
	}
}

// Block cuts every live connection and refuses new ones until Unblock.
func (p *Proxy) Block() {
	p.mu.Lock()
	p.blocked = true
	p.mu.Unlock()
	p.Cut()
}

// Unblock lets clients connect again.
func (p *Proxy) Unblock() {
	p.mu.Lock()
	p.blocked = false
	p.mu.Unlock()
}

// Close shuts the proxy down.
func (p *Proxy) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.ln.Close()
	p.Cut()
}

// Eventually polls cond every few milliseconds until it holds or the
// timeout elapses, and reports whether it held.
func Eventually(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
}
