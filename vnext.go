package nats

import (
	// "context"

	"github.com/nats-io/nats.go/internal/vnext"
)

// VNextClient is an interface that represents a next version
// of the client but that cannot be depended upon yet, mostly used to
// to scaffold packages that may try to start using it already
// and internal development of interfaces from the future.
type VNextClient interface {
	// Connect(string, ...ConnectOption) (vnext.Conn, error)
	// Interface that cannot be implemented outside of the nats package.
	private()
}

// VNext returns the context of a V2 client that can connect.
func VNext() VNextClient {
	return &v2alpha{}
}

// v2alpha is the internal v2 client implementation.
type v2alpha struct {}

// Connect is compatible with nats.go v1 Connect but takes functional option
// interfaces instead for more flexibility.
func (v2alpha) Connect(url string, opts ...ConnectOption) (vnext.Conn, error) {
	nc, err := Connect(url, connectOptions(opts...))
	if err != nil {
		return nil, err
	}
	return &v2Conn{nc}, nil
}

// private makes it so that the interface cannot be implemented upon yet.
func (v2alpha) private() {}

type vnextMsg struct {
	*Msg
}

func (msg *vnextMsg) Subject() string {
	return msg.Msg.Subject
}

func (msg *vnextMsg) Reply() string {
	return msg.Msg.Reply
}

func (msg *vnextMsg) Data() []byte {
	return msg.Msg.Data
}

func (msg *vnextMsg) Header() vnext.Header {
	return msg.Msg.Header
}

func (msg *vnextMsg) Respond(data []byte) error {
	return msg.Msg.Respond(data)
}

////////////////////////////////////////
//                                    //
// Base NATS V2 Client implementation //
//                                    //
////////////////////////////////////////

// v1 MsgHandler can act as a vnext.Handler.
var _ vnext.Handler = MsgHandler(func(*Msg) {})
var _ vnext.Conn = &v2Conn{}
var _ vnext.Msg = &vnextMsg{}

// v2Conn is a possible implementation of a next gen client.
type v2Conn struct {
	nc *Conn
}

func (vc *v2Conn) Publish(subj string, data []byte) error {
	return vc.nc.Publish(subj, data)
}

func (vc *v2Conn) PublishRequest(subj, reply string, data []byte) error {
	return vc.nc.PublishRequest(subj, reply, data)
}

func (vc *v2Conn) PublishMsg(vnext.Msg) error {
	return nil
}

func (vc *v2Conn) Subscribe(subj string, cb vnext.Handler) (vnext.Subscription, error) {
	var (
		sub vnext.Subscription
		err error
	)
	switch fn := cb.(type) {
	case MsgHandler, vnext.MsgHandler:
		_, err = vc.nc.Subscribe(subj, func(msg *Msg) {
			fn.ProcessMsg(&vnextMsg{msg})
		})
	}
	if err != nil {
		return nil, err
	}
	return sub, nil
}

func (vc *v2Conn) Drain() error {
	return nil
}

func (vc *v2Conn) Close() {
}

func (vc *v2Conn) QueueSubscribe(subj, queue string, cb vnext.Handler) (vnext.Subscription, error) {
	_, err := vc.nc.QueueSubscribe(subj, queue, nil)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

///////////////////////////////////////
//                                   //
//  Enhanced types for compatibility //
//                                   //
///////////////////////////////////////

// Takes a collection of connect options and turns them into
// a series of ConnectOption interfaces.
func connectOptions(options ...ConnectOption) Option {
	return func(o *Options) error {
		for _, opt := range options {
			if opt != nil {
				if err := opt.ConfigureConnect(o); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

// Context based APIs interface.
// nc.Context(ctx).Publish()

// // Converts nats.MsgHandler to an interface type that is
// // composable.
// func WithMsgHandler(cb MsgHandler) vnext.Handler {
// 	return nil
// }

// ConfigureConnect implements the ConnectOption interface.
func (fn Option) ConfigureConnect(opts *Options) error {
	return fn(opts)
}

// ConnectOption is an option to configure the connection.
type ConnectOption interface {
	ConfigureConnect(*Options) error
}

// ProcessMsg implements the vnext.Handler interface for
// the regular MsgHandler callbacks.
func (fn MsgHandler) ProcessMsg(msg vnext.Msg) {
	// TODO: Lost metadata and inner state?
	// How to use Respond() without the connection?
	m := &Msg{
		Subject: msg.Subject(),
		Reply:   msg.Reply(),
		Data:    msg.Data(),
		Header:  msg.Header().(Header),
	}
	fn(m)
}
