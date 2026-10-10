package harness

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	zizek "github.com/wallyqs/zizek.go"
)

func TestProxyReconnect(t *testing.T) {
	s := RunServer(t, true)
	p := NewProxy(t, s.Addr())
	reconnected := make(chan struct{}, 4)
	nc, err := nats.Connect(p.URL(), nats.ReconnectWait(10*time.Millisecond),
		nats.ReconnectHandler(func(*nats.Conn) { reconnected <- struct{}{} }))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	p.Cut()
	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("no reconnect after cut")
	}
	s.Restart()
	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("no reconnect after restart")
	}
	if err := zizek.Check(func(tc *zizek.TestCase) error { return nil }, zizek.Config{Examples: 1}); err != nil {
		t.Fatal(err)
	}
}
