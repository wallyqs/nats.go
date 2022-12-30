package test

import (
	"testing"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/vnext"
)

func V2(nc *nats.Conn) vnext.Client {
	type V2 interface {
		V2() vnext.Client
	}
	return nats.VNext(nc).(V2).V2()
}

func TestVNextClient(t *testing.T) {
	nc, err := nats.Connect("localhost")
	if err != nil {
		t.Fatal(err)
	}
	// v2 := func(c V2) vnext.Client {
	// 	return c.V2()
	// }
	// vnext := nats.VNext(nc).(V2).V2()
	// vc := v2(nats.VNext(nc).(V2))
	vc := V2(nc)
	t.Logf("Got things!!! %+v", vc)
	vc.Publish("asdf", []byte("hello"))
	vc.Subscribe("asdf", vnext.MsgHandlerFunc(func(vnext.Msg) {
		// ---
	}))
}
