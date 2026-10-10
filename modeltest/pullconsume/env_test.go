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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/modeltest/harness"
)

// env is one JetStream server, a NATSProxy in front of it, a client
// connected through the proxy, a stream (subjects "<stream>.>") and a
// durable pull consumer with explicit acks.
type env struct {
	t       testing.TB
	srv     *harness.Server
	px      *NATSProxy
	nc      *nats.Conn
	js      jetstream.JetStream
	stream  jetstream.Stream
	cons    jetstream.Consumer
	pubNC   *nats.Conn // direct connection used to publish, unaffected by the proxy
	pubJS   jetstream.JetStream
	subject string
}

func newEnv(t testing.TB) *env {
	t.Helper()
	e := newEnvOn(t, harness.RunServer(t, true), "S", "C")
	t.Cleanup(e.close)
	return e
}

// newEnvOn sets up a proxy, connections, a stream and a consumer on an
// already running server. The caller owns the result and must close it.
func newEnvOn(t testing.TB, srv *harness.Server, stream, consumer string) *env {
	t.Helper()
	e := &env{t: t, srv: srv}
	e.px = NewNATSProxy(t, srv.Addr())
	var err error
	e.nc, err = nats.Connect(e.px.URL(),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(20*time.Millisecond),
		nats.ReconnectJitter(0, 0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	e.js, err = jetstream.New(e.nc)
	if err != nil {
		t.Fatal(err)
	}
	e.pubNC, err = nats.Connect(srv.URL(), nats.MaxReconnects(-1), nats.ReconnectWait(20*time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	e.pubJS, err = jetstream.New(e.pubNC)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err = e.pubJS.CreateStream(ctx, jetstream.StreamConfig{
		Name: stream, Subjects: []string{stream + ".>"}, Storage: jetstream.FileStorage,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	e.subject = stream + ".a"
	if _, err = e.pubJS.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable: consumer, AckPolicy: jetstream.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	e.stream, err = e.js.Stream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	e.cons, err = e.stream.Consumer(ctx, consumer)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// close tears the client side down and deletes the stream.
func (e *env) close() {
	e.nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = e.pubJS.DeleteStream(ctx, e.stream.CachedInfo().Config.Name)
	e.pubNC.Close()
	e.px.Close()
}

// publish stores n messages in the stream, through the direct connection.
func (e *env) publish(n int) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		if _, err := e.pubJS.Publish(ctx, e.subject, []byte(fmt.Sprintf("m%d", i))); err != nil {
			e.t.Fatalf("publish: %v", err)
		}
	}
}

// reconnects returns the client's reconnect count.
func (e *env) reconnects() uint64 { return e.nc.Stats().Reconnects }

// cutAndWaitReconnect cuts the client's connection at the proxy and waits
// until the client has reconnected.
func (e *env) cutAndWaitReconnect() {
	e.t.Helper()
	before := e.reconnects()
	e.px.Cut()
	if !harness.Eventually(10*time.Second, func() bool {
		return e.reconnects() > before && e.nc.IsConnected()
	}) {
		e.t.Fatalf("client did not reconnect")
	}
}

// isStatus reports whether f is a server->client control message with the
// given status code (e.g. "100", "408").
func isStatus(f *Frame, code string) bool {
	return f.ToClient && f.Op == "HMSG" && f.Status() == code
}

// isStreamMsg reports whether f delivers a stream message to the client.
func isStreamMsg(f *Frame) bool {
	return f.ToClient && strings.HasPrefix(f.Reply, "$JS.ACK.")
}

// closedWithin reports whether the channel returned by cc.Closed() is
// closed within d. Closed() itself takes the subscription lock, so it is
// called on its own goroutine.
func closedWithin(cc jetstream.ConsumeContext, d time.Duration) bool {
	ch := make(chan struct{})
	go func() {
		<-cc.Closed()
		close(ch)
	}()
	select {
	case <-ch:
		return true
	case <-time.After(d):
		return false
	}
}
