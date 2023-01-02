package nats

import (
	"github.com/nats-io/nats.go/internal/vnext"
)

// // VNextConn is an interface that represents the next version
// // of the client but that cannot be depended upon yet, mostly used to
// // to scaffold packages that may try to start using it already.
// type VNextConn interface {
// 	// Interface that cannot be implemented or depended upon
// 	// outside of the nats package.
// 	private()
// }

type MsgHandlerI interface {
	MsgHandler
}

func WithMsgHandler(cb MsgHandler) vnext.Handler {
	return nil
}

// VNext returns an interface of a possible next version of the NATS client APIs.
func VNext(nc *Conn) vnext.Conn {
	return &vnextClient{nc, nil}
}

// type vnextMsg struct {
// 	*Msg
// }

// func(*vnextMsg) ProcessMsg(msg vnext.Msg) {}

type mHandler struct {
	cb MsgHandler
}

type msgHandler interface {
	processMsg(*Msg)
}

// MsgHandler implements both an internal interface
func (fn MsgHandler) processMsg(msg *Msg) { fn(msg) }
func (fn MsgHandler) ProcessMsg(msg vnext.Msg) {
	m := &Msg{
		Subject: msg.Subject(),
		Reply: msg.Reply(),
		Data: msg.Data(),
		Header: msg.Header().(Header),
	}
	fn(m)
}

// MsgHandler can act as a vnext.Handler
var _ vnext.Handler = MsgHandler(func(*Msg){})

// func(fn msgHandler) processMsg(msg *Msg) {
// 	fn(msg)
// }

// func(fn msgHandler) ProcessMsg(msg vnext.Msg) {
// 	fn(msg)
// }

// vnextClient is an implementation of the next gen client.
type vnextClient struct {
	nc *Conn
	vnext.Conn
}

func (vc *vnextClient) Publish(subj string, data []byte) error {
	return nil
}

func (vc *vnextClient) PublishRequest(subj, reply string, data []byte) error {
	return nil
}

func (vc *vnextClient) PublishMsg(vnext.Msg) error {
	return nil
}

func (vc *vnextClient) Subscribe(subj string, cb vnext.Handler) (vnext.Subscription, error) {
	// ---
	_, err := vc.nc.Subscribe(subj, nil)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// func (vc *vnextClient) QueueSubscribe(subj, queue string, cb vnext.Handler) (vnext.Subscription, error) {
// 	_, err := vc.nc.QueueSubscribe(subj, queue, cb)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return nil, nil
// }

//
// type V2Client = vnext.Client
//
// type v2client struct {
// 	*Conn
// 	vnext.Client
// }
//
// func (nc *Conn) V2() V2Client {
// 	return &v2client{Conn: nc}
// }
//
