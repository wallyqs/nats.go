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
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeServer is a scripted NATS server speaking just enough of the client
// protocol for request/reply: it records SUB/UNSUB/PUB from one client and
// lets the test write arbitrary MSG/HMSG frames back. This is how the
// model's environment actions that a real server never performs on cue
// (a reply for a subject the mux does not own, a reply exactly when the
// test wants it, a 503 to a chosen inbox) are injected.
type fakeServer struct {
	t  testing.TB
	ln net.Listener

	mu    sync.Mutex
	cond  *sync.Cond
	conn  net.Conn
	subs  map[string]string // sid -> subject
	nsub  []string          // every SUB subject, in order
	pubs  []fakePub
	wmu   sync.Mutex
	close bool
}

type fakePub struct {
	Subject, Reply string
	Data           []byte
}

func newFakeServer(t testing.TB) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake server listen: %v", err)
	}
	fs := &fakeServer{t: t, ln: ln, subs: map[string]string{}}
	fs.cond = sync.NewCond(&fs.mu)
	go fs.accept()
	t.Cleanup(fs.Close)
	return fs
}

func (fs *fakeServer) URL() string { return "nats://" + fs.ln.Addr().String() }

func (fs *fakeServer) Close() {
	fs.mu.Lock()
	fs.close = true
	c := fs.conn
	fs.cond.Broadcast()
	fs.mu.Unlock()
	fs.ln.Close()
	if c != nil {
		c.Close()
	}
}

func (fs *fakeServer) accept() {
	for {
		c, err := fs.ln.Accept()
		if err != nil {
			return
		}
		fs.mu.Lock()
		fs.conn = c
		fs.mu.Unlock()
		go fs.serve(c)
	}
}

func (fs *fakeServer) write(s string) {
	fs.wmu.Lock()
	defer fs.wmu.Unlock()
	fs.mu.Lock()
	c := fs.conn
	fs.mu.Unlock()
	if c != nil {
		_, _ = io.WriteString(c, s)
	}
}

func (fs *fakeServer) serve(c net.Conn) {
	fs.write(`INFO {"server_id":"FAKE","server_name":"fake","version":"2.11.0","proto":1,"headers":true,"max_payload":1048576,"host":"127.0.0.1","port":0}` + "\r\n")
	r := bufio.NewReader(c)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		switch strings.ToUpper(f[0]) {
		case "CONNECT":
		case "PING":
			fs.write("PONG\r\n")
		case "PONG":
		case "SUB":
			sid := f[len(f)-1]
			fs.mu.Lock()
			fs.subs[sid] = f[1]
			fs.nsub = append(fs.nsub, f[1])
			fs.cond.Broadcast()
			fs.mu.Unlock()
		case "UNSUB":
			fs.mu.Lock()
			if len(f) == 2 {
				delete(fs.subs, f[1])
			}
			fs.cond.Broadcast()
			fs.mu.Unlock()
		case "PUB", "HPUB":
			size, _ := strconv.Atoi(f[len(f)-1])
			buf := make([]byte, size+2)
			if _, err := io.ReadFull(r, buf); err != nil {
				return
			}
			p := fakePub{Subject: f[1], Data: buf[:size]}
			hdr := 0
			if f[0] == "HPUB" {
				hdr, _ = strconv.Atoi(f[len(f)-2])
				if len(f) == 5 {
					p.Reply = f[2]
				}
			} else if len(f) == 4 {
				p.Reply = f[2]
			}
			p.Data = p.Data[hdr:]
			fs.mu.Lock()
			fs.pubs = append(fs.pubs, p)
			fs.cond.Broadcast()
			fs.mu.Unlock()
		}
	}
}

// waitFor blocks until cond (evaluated under fs.mu) holds or times out.
func (fs *fakeServer) waitFor(what string, cond func() bool) {
	fs.t.Helper()
	done := make(chan struct{})
	timer := time.AfterFunc(5*time.Second, func() {
		fs.mu.Lock()
		close(done)
		fs.cond.Broadcast()
		fs.mu.Unlock()
	})
	defer timer.Stop()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for !cond() {
		select {
		case <-done:
			fs.t.Fatalf("fake server: timed out waiting for %s", what)
		default:
		}
		fs.cond.Wait()
	}
}

// WaitSub returns the sid of the subscription whose subject has the prefix.
func (fs *fakeServer) WaitSub(prefix string) (sid, subject string) {
	fs.t.Helper()
	fs.waitFor("SUB "+prefix, func() bool {
		for s, subj := range fs.subs {
			if strings.HasPrefix(subj, prefix) {
				sid, subject = s, subj
				return true
			}
		}
		return false
	})
	return sid, subject
}

// WaitPub returns the n-th (0-based) PUB to subject.
func (fs *fakeServer) WaitPub(subject string, n int) fakePub {
	fs.t.Helper()
	var p fakePub
	fs.waitFor(fmt.Sprintf("PUB %s #%d", subject, n), func() bool {
		i := 0
		for _, q := range fs.pubs {
			if q.Subject == subject {
				if i == n {
					p = q
					return true
				}
				i++
			}
		}
		return false
	})
	return p
}

// Msg delivers a MSG frame on sid.
func (fs *fakeServer) Msg(subject, sid string, data []byte) {
	fs.write(fmt.Sprintf("MSG %s %s %d\r\n%s\r\n", subject, sid, len(data), data))
}

// NoResponders delivers the server's 503 status to subject on sid.
func (fs *fakeServer) NoResponders(subject, sid string) {
	hdr := "NATS/1.0 503\r\n\r\n"
	fs.write(fmt.Sprintf("HMSG %s %s %d %d\r\n%s\r\n", subject, sid, len(hdr), len(hdr), hdr))
}

// SubCount reports how many SUB frames named a subject with the prefix.
func (fs *fakeServer) SubCount(prefix string) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n := 0
	for _, s := range fs.nsub {
		if strings.HasPrefix(s, prefix) {
			n++
		}
	}
	return n
}

// PubCount reports how many PUB frames went to subject.
func (fs *fakeServer) PubCount(subject string) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n := 0
	for _, p := range fs.pubs {
		if p.Subject == subject {
			n++
		}
	}
	return n
}
