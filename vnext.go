package nats

import (
	"fmt"
	"github.com/nats-io/nats.go/internal/vnext"
)

// VNextClient is an interface that represents the next version
// of the client.
type VNextClient interface {
	// Interface that cannot be implemented or depended upon yet
	// outside of the nats package.
	private()
}

// VNext returns the next version of the NATS client APIs.
func VNext(nc *Conn) VNextClient {
	return &vnextClient{nc}
}

// vnextClient is an implementation of the next gen client.
type vnextClient struct {
	nc *Conn
}

func (*vnextClient) private() {}

func (vc *vnextClient) V2() vnext.Client {
	return &v2Client{vc, nil}
}

type v2Client struct {
	vc *vnextClient
	vnext.Client
}

func (*v2Client) Publish(subj string, data []byte) {}
func (*v2Client) Subscribe(subj string, cb vnext.MsgHandler) (vnext.Subscription, error) {
	fmt.Println("aaaaaaaaaaa", cb)
	return nil, nil
}

// type V2Client = vnext.Client

// type v2client struct {
// 	*Conn
// 	vnext.Client
// }

// func (nc *Conn) V2() V2Client {
// 	return &v2client{Conn: nc}
// }
