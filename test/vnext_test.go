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

// Generic handler.
type handlerExample struct {
}

func (handlerExample) ProcessMsg(msg vnext.Msg) {
	fmt.Println("Called!!", msg)
}

func TestVNextClient(t *testing.T) {
	v2, err := nats.Connect("localhost")
	if err != nil {
		t.Fatal(err)
	}
	// Grab internal implementation of next version of the client.
	nc := nats.VNext(v2)
	t.Logf("Got things!!! %+v", nc)
	nc.Publish("asdf", []byte("hello"))
	// nc.Subscribe("asdf", vnext.MsgHandlerFunc(func(vnext.Msg) {
	// 	// ---
	// }))
	// Plain will not work
	// nc.Subscribe("foo", func(vnext.Msg) {
	// 	// ---
	// })

	// MsgHandler is of type func(vnext.Msg) which implements Handler interface.
	// In the future, casting via vnext.MsgHandler may not be necessary since Go will auto implement
	// one line function interfaces.
	nc.Subscribe("foo", vnext.MsgHandler(func(msg vnext.Msg) {
		t.Logf("Got: %+v", msg)
	}))

	type handler interface {
		ProcessMsg(vnext.Msg)
	}

	// 
	bar := vnext.MsgHandler(func(vnext.Msg) {
		// Use new Msg interface type.
	})
	nc.Subscribe("bar", bar)

	// Need to set the type explicitly in the callback.
	var baz vnext.MsgHandler = func(msg vnext.Msg) {
	}
	nc.Subscribe("bar", baz)
	// nc.Subscribe("bar", vnext.MsgHandler(handler(&handlerExample{}).(vnext.MsgHandler)))
	nc.Subscribe("bar", vnext.Handler(handler(handlerExample{}).(vnext.Handler)))
	// nc.Subscribe("bar", vnext.MsgHandler())
	nc.Subscribe("bar", handlerExample{})

	nc.Subscribe("bar", nats.MsgHandler(func(msg *nats.Msg){
		fmt.Println("ok!", msg)
	}))

	// MsgHandler can act as a regular message handler.
	nc.Subscribe("bar", nats.MsgHandler(func(msg *nats.Msg){
		fmt.Println("ok!", msg)
	}))

	// These work with regular callback syntax.
	a := struct{
		name string
		handler vnext.MsgHandler
	}{
		name: "From Foo",
		handler:  func(msg vnext.Msg){
			fmt.Println("Got: ", msg)
		},
	}
	nc.Subscribe("baz", a.handler)

	b := struct{
		name string
		handler vnext.MsgHandler
	}{ name: "From Foo" }
	b.handler = func(msg vnext.Msg){
		fmt.Println("Got: ", msg, a.name)
	}
	nc.Subscribe("baz", b.handler)
}
