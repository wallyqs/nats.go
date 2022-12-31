package test

import (
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/vnext"
)

// func V2(nc *nats.Conn) vnext.Client {
// 	type V2 interface {
// 		V2() vnext.Client
// 	}
// 	return nats.VNext(nc).Connect()
// }

type handlerExample struct {
}

func (handlerExample) ProcessMsg(msg vnext.Msg) {
	fmt.Println("Called!!", msg)
}

func TestVNextClient(t *testing.T) {
	v2, err := nats.VNext().Connect("localhost")
	if err != nil {
		t.Fatal(err)
	}
	// Grab internal implementation of next version of the client.
	nc := v2.(vnext.Client)
	// v2 := func(c V2) vnext.Client {
	// 	return c.V2()
	// }
	// vnext := nats.VNext(nc).(V2).V2()
	// vc := v2(nats.VNext(nc).(V2))
	t.Logf("Got things!!! %+v", nc)
	nc.Publish("asdf", []byte("hello"))
	// nc.Subscribe("asdf", vnext.MsgHandlerFunc(func(vnext.Msg) {
	// 	// ---
	// }))
	// Plain will not work
	// nc.Subscribe("foo", func(vnext.Msg) {
	// 	// ---
	// })
	nc.Subscribe("foo", vnext.MsgHandler(func(msg vnext.Msg) {
		t.Logf("Got: %+v", msg)
	}))

	type handler interface {
		ProcessMsg(vnext.Msg)
	}
	bar := vnext.MsgHandler(func(vnext.Msg) {
		// ---
	})

	// Need to set the type explicitly in the callback.
	var baz vnext.MsgHandler = func(msg vnext.Msg) {
	}
	nc.Subscribe("bar", baz)
	// nc.Subscribe("bar", vnext.MsgHandler(handler(&handlerExample{}).(vnext.MsgHandler)))
	nc.Subscribe("bar", vnext.Handler(handler(handlerExample{}).(vnext.Handler)))
	// nc.Subscribe("bar", vnext.MsgHandler())
	nc.Subscribe("bar", bar)
	nc.Subscribe("bar", handlerExample{})

}
