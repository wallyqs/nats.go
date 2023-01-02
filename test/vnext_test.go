package test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/vnext"
)

// func V2(nc *nats.Conn) vnext.Client {
// 	type V2 interface {
// 		V2() vnext.Client
// 	}
// 	return nats.VNext(nc).Connect()
// }

// // Generic handler.
// type handlerExample struct {
// }

// func (handlerExample) ProcessMsg(msg vnext.Msg) {
// 	fmt.Println("Called!!", msg)
// }

// func TestVNextClient(t *testing.T) {
// 	// v2, err := nats.Connect("localhost")
// 	// if err != nil {
// 	// 	t.Fatal(err)
// 	// }
// 	// // Grab internal implementation of next version of the client.
// 	// nc := nats.VNext(v2)
// 	// t.Logf("Got things!!! %+v", nc)
// 	// nc.Publish("asdf", []byte("hello"))
// 	// // nc.Subscribe("asdf", vnext.MsgHandlerFunc(func(vnext.Msg) {
// 	// // 	// ---
// 	// // }))
// 	// // Plain will not work
// 	// // nc.Subscribe("foo", func(vnext.Msg) {
// 	// // 	// ---
// 	// // })

// 	// // MsgHandler is of type func(vnext.Msg) which implements Handler interface.
// 	// // In the future, casting via vnext.MsgHandler may not be necessary since Go will auto implement
// 	// // one line function interfaces.
// 	// nc.Subscribe("foo", vnext.MsgHandler(func(msg vnext.Msg) {
// 	// 	t.Logf("Got: %+v", msg)
// 	// }))

// 	// type handler interface {
// 	// 	ProcessMsg(vnext.Msg)
// 	// }

// 	// //
// 	// bar := vnext.MsgHandler(func(vnext.Msg) {
// 	// 	// Use new Msg interface type.
// 	// })
// 	// nc.Subscribe("bar", bar)

// 	// // Need to set the type explicitly in the callback.
// 	// var baz vnext.MsgHandler = func(msg vnext.Msg) {
// 	// }
// 	// nc.Subscribe("bar", baz)
// 	// // nc.Subscribe("bar", vnext.MsgHandler(handler(&handlerExample{}).(vnext.MsgHandler)))
// 	// nc.Subscribe("bar", vnext.Handler(handler(handlerExample{}).(vnext.Handler)))
// 	// // nc.Subscribe("bar", vnext.MsgHandler())
// 	// nc.Subscribe("bar", handlerExample{})

// 	// nc.Subscribe("bar", nats.MsgHandler(func(msg *nats.Msg){
// 	// 	fmt.Println("ok!", msg)
// 	// }))

// 	// // MsgHandler can act as a regular message handler.
// 	// nc.Subscribe("bar", nats.MsgHandler(func(msg *nats.Msg){
// 	// 	fmt.Println("ok!", msg)
// 	// }))

// 	// // These work with regular callback syntax.
// 	// a := struct{
// 	// 	name string
// 	// 	handler vnext.MsgHandler
// 	// }{
// 	// 	name: "From Foo",
// 	// 	handler:  func(msg vnext.Msg){
// 	// 		fmt.Println("Got: ", msg)
// 	// 	},
// 	// }
// 	// nc.Subscribe("baz", a.handler)

// 	// b := struct{
// 	// 	name string
// 	// 	handler vnext.MsgHandler
// 	// }{ name: "From Foo" }
// 	// b.handler = func(msg vnext.Msg){
// 	// 	fmt.Println("Got: ", msg, a.name)
// 	// }
// 	// nc.Subscribe("baz", b.handler)
// }

type V2Alpha interface {
	Connect(string, ...nats.ConnectOption) (vnext.Conn, error)
}

func TestV2Client(t *testing.T) {
	// Surface the work in progress version of the NATS client.
	v2 := nats.VNext().(V2Alpha)
	nc, err := v2.Connect("localhost", nats.Name("v2:alpha:client"))
	if err != nil {
		t.Fatal(err)
	}
	// Generics for the handlers? (type or Msg)
	nc.Publish("asdf", []byte("hello"))

	// Subscribe async with interface type callbacks.
	nc.Subscribe("asdf", vnext.MsgHandler(func(msg vnext.Msg) {
		t.Logf("v2 style: Got a message on %q: %q", msg.Subject(), msg.Data())
		msg.Respond([]byte("Hello World!"))
	}))

	// Subscribe async with v1 style MsgHandler.
	var v1cb nats.MsgHandler = func(msg *nats.Msg) {
		t.Logf("v1 style: Got a message on %q: %q", msg.Subject, msg.Data)
		msg.Respond([]byte("Hello World!"))
	}
	nc.Subscribe("foo", v1cb)

	// breaking change: anonymous function callback has to be casted into MsgHandler type.
	nc.Subscribe("foo", nats.MsgHandler(func(msg *nats.Msg) {
		t.Logf("v1 style handler: Got a message on %q: %q", msg.Subject, msg.Data)
		msg.Respond([]byte("Hello World!"))
	}))
	nc.Publish("asdf", []byte("hello"))
	nc.Publish("foo", []byte("hello"))
	time.Sleep(1 * time.Second)
	// nc.QueueSubscribe("foo", "bar", vnext.MsgHandler(func(msg vnext.Msg){
	// }))
	// nc.Close()
	// // nc.Subscribe("asdf", vnext.MsgHandlerFunc(func(vnext.Msg) {
	// // 	// ---
	// // }))
	// // Plain will not work
	// // nc.Subscribe("foo", func(vnext.Msg) {
	// // 	// ---
	// // })

	// // MsgHandler is of type func(vnext.Msg) which implements Handler interface.
	// // In the future, casting via vnext.MsgHandler may not be necessary since Go will auto implement
	// // one line function interfaces.
	// nc.Subscribe("foo", vnext.MsgHandler(func(msg vnext.Msg) {
	// 	t.Logf("Got: %+v", msg)
	// }))

	// type handler interface {
	// 	ProcessMsg(vnext.Msg)
	// }

	// //
	// bar := vnext.MsgHandler(func(vnext.Msg) {
	// 	// Use new Msg interface type.
	// })
	// nc.Subscribe("bar", bar)

	// // Need to set the type explicitly in the callback.
	// var baz vnext.MsgHandler = func(msg vnext.Msg) {
	// }
	// nc.Subscribe("bar", baz)
	// // nc.Subscribe("bar", vnext.MsgHandler(handler(&handlerExample{}).(vnext.MsgHandler)))
	// nc.Subscribe("bar", vnext.Handler(handler(handlerExample{}).(vnext.Handler)))
	// // nc.Subscribe("bar", vnext.MsgHandler())
	// nc.Subscribe("bar", handlerExample{})

	// nc.Subscribe("bar", nats.MsgHandler(func(msg *nats.Msg){
	// 	fmt.Println("ok!", msg)
	// }))

	// // MsgHandler can act as a regular message handler.
	// nc.Subscribe("bar", nats.MsgHandler(func(msg *nats.Msg){
	// 	fmt.Println("ok!", msg)
	// }))

	// // These work with regular callback syntax.
	// a := struct{
	// 	name string
	// 	handler vnext.MsgHandler
	// }{
	// 	name: "From Foo",
	// 	handler:  func(msg vnext.Msg){
	// 		fmt.Println("Got: ", msg)
	// 	},
	// }
	// nc.Subscribe("baz", a.handler)

	// b := struct{
	// 	name string
	// 	handler vnext.MsgHandler
	// }{ name: "From Foo" }
	// b.handler = func(msg vnext.Msg){
	// 	fmt.Println("Got: ", msg, a.name)
	// }
	// nc.Subscribe("baz", b.handler)
}
