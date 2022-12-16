package test

import (
	"fmt"
	"testing"

	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

type TestClient interface {
	SetConnectionStatus(nats.Status)
}

func TestInternal(t *testing.T) {
	opts := test.DefaultTestOptions
	opts.Port = 4222
	s := RunServerWithOptions(opts)
	defer s.Shutdown()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatal(err)
	}
	tc := nats.TestClient(nc)
	fmt.Println(tc)
	tc.SetConnectionStatus(1)
	nc.Close()
}

func TestRequestInit(t *testing.T) {
	// o := natsserver.DefaultTestOptions
	// o.Port = -1
	// s := RunServerWithOptions(&o)
	// defer s.Shutdown()

	// nc, err := Connect(s.ClientURL())
	// if err != nil {
	// 	t.Fatalf("Error on connect: %v", err)
	// }
	// defer nc.Close()

	// if _, err := nc.Subscribe("foo", func(m *Msg) {
	// 	m.Respond([]byte("reply"))
	// }); err != nil {
	// 	t.Fatalf("Error on subscribe: %v", err)
	// }

	// // Artificially change the status to something that would make the internal subscribe
	// // call fail. Don't use CLOSED because then there is a risk that the flusher() goes away
	// // and so the rest of the test would fail.
	// nc.mu.Lock()
	// orgStatus := nc.status
	// nc.status = DRAINING_SUBS
	// nc.mu.Unlock()

	// if _, err := nc.Request("foo", []byte("request"), 50*time.Millisecond); err == nil {
	// 	t.Fatal("Expected error, got none")
	// }

	// nc.mu.Lock()
	// nc.status = orgStatus
	// nc.mu.Unlock()

	// if _, err := nc.Request("foo", []byte("request"), 500*time.Millisecond); err != nil {
	// 	t.Fatalf("Error on request: %v", err)
	// }
}
