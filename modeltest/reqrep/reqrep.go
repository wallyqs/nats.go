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

// Package reqrep holds the tests derived from the RequestReply TLA+ model
// (models/RequestReply): core request/reply in nats.go and context.go.
//
// The model's invariants talk about nc.respMap and nc.respMux, which are
// unexported. Peek reads them the only way a test outside package nats can:
// through reflect + unsafe, while holding nc.mu exactly like the library
// does, so the race detector sees a properly synchronized read. It is a
// read-only observation and never mutates the connection.
package reqrep

import (
	"reflect"
	"sync"
	"unsafe"

	"github.com/nats-io/nats.go"
)

// MuxState is a snapshot of the new-style request machinery of a Conn.
type MuxState struct {
	// RespMapLen is len(nc.respMap): requests whose response channel is
	// still registered.
	RespMapLen int
	// HasMux reports nc.respMux != nil.
	HasMux bool
	// MuxValid reports whether nc.respMux is a live subscription (only
	// meaningful when HasMux).
	MuxValid bool
	// MuxSubject is nc.respMux.Subject ("" when HasMux is false).
	MuxSubject string
}

func field(nc *nats.Conn, name string) unsafe.Pointer {
	f := reflect.ValueOf(nc).Elem().FieldByName(name)
	if !f.IsValid() {
		panic("nats.Conn has no field " + name + "; update modeltest/reqrep")
	}
	return unsafe.Pointer(f.UnsafeAddr())
}

// Peek snapshots nc.respMap / nc.respMux under nc.mu.
func Peek(nc *nats.Conn) MuxState {
	mu := (*sync.RWMutex)(field(nc, "mu"))
	respMap := (*map[string]chan *nats.Msg)(field(nc, "respMap"))
	respMux := (**nats.Subscription)(field(nc, "respMux"))

	mu.RLock()
	st := MuxState{RespMapLen: len(*respMap)}
	mux := *respMux
	mu.RUnlock()

	if mux != nil {
		st.HasMux = true
		st.MuxValid = mux.IsValid()
		st.MuxSubject = mux.Subject
	}
	return st
}
