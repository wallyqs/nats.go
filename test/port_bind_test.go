package test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// portBindDialer implements nats.CustomDialer and binds to a specific local address/port.
type portBindDialer struct {
	localAddr  *net.TCPAddr
	timeout    time.Duration
	dialedFrom net.Addr
}

func (d *portBindDialer) Dial(network, address string) (net.Conn, error) {
	dialer := &net.Dialer{
		LocalAddr: d.localAddr,
		Timeout:   d.timeout,
	}
	conn, err := dialer.Dial(network, address)
	if err == nil {
		d.dialedFrom = conn.LocalAddr()
	}
	return conn, err
}

func TestCustomDialerPortBinding(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	// Find a free port to bind to.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to find free port: %v", err)
	}
	freePort := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	localAddr := &net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: freePort,
	}

	dialer := &portBindDialer{
		localAddr: localAddr,
		timeout:   2 * time.Second,
	}

	nc, err := nats.Connect(nats.DefaultURL, nats.SetCustomDialer(dialer))
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	// Verify we can publish and subscribe.
	sub, err := nc.SubscribeSync("test")
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	if err := nc.Publish("test", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	nc.Flush()

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error getting message: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Expected 'hello', got '%s'", string(msg.Data))
	}

	// Verify the dialer bound to the expected local port.
	if dialer.dialedFrom == nil {
		t.Fatal("Expected dialedFrom to be set")
	}

	_, port, err := net.SplitHostPort(dialer.dialedFrom.String())
	if err != nil {
		t.Fatalf("Failed to parse local address: %v", err)
	}
	expectedPort := fmt.Sprintf("%d", freePort)
	if port != expectedPort {
		t.Fatalf("Expected local port %s, got %s", expectedPort, port)
	}

	// Also verify the server sees the client from the expected port.
	clients, err := s.Connz(nil)
	if err != nil {
		t.Fatalf("Error getting connz: %v", err)
	}
	if clients == nil || len(clients.Conns) == 0 {
		t.Fatal("Expected at least one client connection")
	}

	found := false
	for _, ci := range clients.Conns {
		if ci.Port == freePort {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Expected server to see client connected from port %d", freePort)
	}

	t.Logf("Successfully connected from local address: %s (port %d)", dialer.dialedFrom.String(), freePort)
}
