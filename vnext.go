package nats

import (
	"fmt"

	"github.com/nats-io/nats.go/internal/vnext"
)

// VNextClient is an interface that represents the next version
// of the client but that cannot be depended upon yet, mostly used to
// to scaffold packages that may try to start using it already.
type VNextClient interface {
	Connect(url string, options ...Option) (VNextClient, error)
	// Interface that cannot be implemented or depended upon
	// outside of the nats package.
	private()
}

// VNext returns the next version of the NATS client APIs.
func VNext() VNextClient {
	return &vnextClient{}
}

// vnextClient is an implementation of the next gen client.
type vnextClient struct {
	nc *Conn
}

func (*vnextClient) private() {}

// Connect takes ConnectOption interface instead, and Option implements that.
func (vc *vnextClient) Connect(url string, options ...Option) (VNextClient, error) {
	if vc == nil {
		return nil, fmt.Errorf("nats: invalid call to vnext client")
	}
	nc, err := Connect(url, options...)
	if err != nil {
		return nil, err
	}
	return &v2Client{nc, vc, nil, nil}, nil
}

type v2Client struct {
	nc *Conn
	vc *vnextClient
	VNextClient
	vnext.Client
}

func (*v2Client) Publish(subj string, data []byte) error {
	return nil
}

func (*v2Client) PublishRequest(subj, reply string, data []byte) error {
	return nil
}

func (*v2Client) PublishMsg(vnext.Msg) error {
	return nil
}

func (*v2Client) Subscribe(subj string, cb vnext.Handler) (vnext.Subscription, error) {
	fmt.Println("aaaaaaaaaaa", subj, cb)
	return nil, nil
}

func (*v2Client) QueueSubscribe(subj, queue string, cb vnext.Handler) (vnext.Subscription, error) {
	fmt.Println("bbbbbbbbbbb", subj, queue, cb)
	return nil, nil
}

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
