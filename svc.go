package nats

import (
	// NATS client uses svc package to create microservice.
	"github.com/nats-io/nats.go/svc"
	// Client implements some common types from svc package.
	svc_types "github.com/nats-io/nats.go/svc/types"
)

// AddService takes a service configuration and returns a Service.
func (nc *Conn) AddService(conf svc.Config) (svc.Service, error) {
	return svc.Add(nc.Edge(), conf)
}

//////////////////////////
//                      //
//  Experimental APIs   //
//                      //
//////////////////////////

// Edge enables the experimental APIs from the client. Should not be used directly by an application
// it represents possible APIs that may be part of *nats.Conn in the future.
func (nc *Conn) Edge() *EdgeClient {
	return &EdgeClient{nc}
}

// EdgeClient encapsulates experimental APIs behavior.  It should not be
// be used directly in an application.
type EdgeClient struct {
	nc *Conn
}

// serviceMsg implements the shared interface at svc_types.
type serviceMsg struct {
	m *Msg
}

func (msg *serviceMsg) Data() []byte {
	return msg.m.Data
}

func (msg *serviceMsg) Reply() string {
	return msg.m.Reply
}

func (msg *serviceMsg) Respond(payload []byte) error {
	return msg.m.Respond(payload)
}

func (msg *serviceMsg) RespondMsg(smsg svc_types.Msg) error {
	nmsg := &Msg{
		Data: smsg.Data(),
	}
	return msg.m.RespondMsg(nmsg)
}

// serviceSub implements the shared interface at svc_types.
type serviceSub struct {
	s *Subscription
}

func (sub *serviceSub) Drain() error {
	return sub.s.Drain()
}

func (sub *serviceSub) Unsubscribe() error {
	return sub.s.Unsubscribe()
}

// QueueSubscribe creates an queue subscription used to create services.
func (ec *EdgeClient) QueueSubscribe(subj, queue string, cb svc_types.MsgHandler) (svc_types.Subscription, error) {
	nsub, err := ec.nc.QueueSubscribe(subj, queue, func(m *Msg) {
		cb(&serviceMsg{m})
	})
	if err != nil {
		return nil, err
	}
	return &serviceSub{nsub}, nil
}
