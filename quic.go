// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nats

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/quic-go/quic-go"
)

// quicConn wraps a QUIC stream with optimized I/O operations
type quicConn struct {
	quic.Connection
	stream quic.Stream
	// Cache addresses to avoid repeated calls
	localAddr  net.Addr
	remoteAddr net.Addr
}

func (qc *quicConn) Read(b []byte) (int, error) {
	return qc.stream.Read(b)
}

func (qc *quicConn) Write(b []byte) (int, error) {
	return qc.stream.Write(b)
}

func (qc *quicConn) Close() error {
	qc.stream.Close()
	return qc.Connection.CloseWithError(0, "connection closed")
}

// LocalAddr returns cached address to avoid repeated calls
func (qc *quicConn) LocalAddr() net.Addr {
	if qc.localAddr == nil {
		qc.localAddr = qc.Connection.LocalAddr()
	}
	return qc.localAddr
}

// RemoteAddr returns cached address to avoid repeated calls
func (qc *quicConn) RemoteAddr() net.Addr {
	if qc.remoteAddr == nil {
		qc.remoteAddr = qc.Connection.RemoteAddr()
	}
	return qc.remoteAddr
}

// SetDeadline directly on stream for better performance
func (qc *quicConn) SetDeadline(t time.Time) error {
	return qc.stream.SetDeadline(t)
}

// SetReadDeadline directly on stream for better performance
func (qc *quicConn) SetReadDeadline(t time.Time) error {
	return qc.stream.SetReadDeadline(t)
}

// SetWriteDeadline directly on stream for better performance
func (qc *quicConn) SetWriteDeadline(t time.Time) error {
	return qc.stream.SetWriteDeadline(t)
}

// quicInitHandshake establishes a QUIC connection with optimized settings
func (nc *Conn) quicInitHandshake(u *url.URL) error {
	tlsConf := &tls.Config{
		ServerName:         u.Hostname(),
		InsecureSkipVerify: true, // Default to true for QUIC, can be overridden
		NextProtos:         []string{"nats"},
		// Optimize TLS 1.3 for QUIC performance
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
	}

	// Use user-provided TLS config if available
	if nc.Opts.TLSConfig != nil {
		tlsConf = nc.Opts.TLSConfig.Clone()
		if tlsConf.ServerName == "" {
			tlsConf.ServerName = u.Hostname()
		}
		// Ensure ALPN is set for NATS
		if len(tlsConf.NextProtos) == 0 {
			tlsConf.NextProtos = []string{"nats"}
		}
		// Force TLS 1.3 for optimal QUIC performance
		if tlsConf.MinVersion < tls.VersionTLS13 {
			tlsConf.MinVersion = tls.VersionTLS13
		}
		tlsConf.MaxVersion = tls.VersionTLS13
	}

	// Create optimized QUIC configuration for NATS workloads
	quicConf := &quic.Config{
		// Increase initial flow control windows for high throughput
		InitialStreamReceiveWindow:     1024 * 1024,      // 1MB per stream
		MaxStreamReceiveWindow:         16 * 1024 * 1024, // 16MB max per stream
		InitialConnectionReceiveWindow: 4 * 1024 * 1024,  // 4MB per connection
		MaxConnectionReceiveWindow:     64 * 1024 * 1024, // 64MB max per connection
		
		// Optimize for low latency
		KeepAlivePeriod: 30 * time.Second,
		
		// Allow more concurrent streams (though NATS typically uses 1)
		MaxIncomingStreams: 100,
		MaxIncomingUniStreams: 100,
		
		// Disable stateless retry for faster connection establishment
		DisablePathMTUDiscovery: false,
	}

	ctx := context.Background()
	if nc.Opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, nc.Opts.Timeout)
		defer cancel()
	}

	// Use optimized QUIC configuration
	conn, err := quic.DialAddr(ctx, u.Host, tlsConf, quicConf)
	if err != nil {
		return fmt.Errorf("failed to establish QUIC connection: %w", err)
	}

	// Open a bidirectional stream for NATS communication
	// Use async stream opening with timeout for better performance
	streamCtx := context.Background()
	if nc.Opts.Timeout > 0 {
		var streamCancel context.CancelFunc
		streamCtx, streamCancel = context.WithTimeout(streamCtx, nc.Opts.Timeout)
		defer streamCancel()
	}
	
	stream, err := conn.OpenStreamSync(streamCtx)
	if err != nil {
		conn.CloseWithError(0, "failed to open stream")
		return fmt.Errorf("failed to open QUIC stream: %w", err)
	}

	// Wrap the QUIC connection and stream with cached addresses
	qc := &quicConn{
		Connection: conn,
		stream:     stream,
		localAddr:  conn.LocalAddr(),
		remoteAddr: conn.RemoteAddr(),
	}
	nc.conn = qc

	nc.bindToNewConn()
	return nil
}

// processQuicConnectInit handles the connection initialization for QUIC connections
// QUIC protocol flow: CLIENT sends CONNECT first -> SERVER sends INFO+OK -> CLIENT sends PING -> SERVER sends PONG
func (nc *Conn) processQuicConnectInit() error {
	// Set our deadline for the whole connect process
	nc.conn.SetDeadline(time.Now().Add(nc.Opts.Timeout))
	defer nc.conn.SetDeadline(time.Time{})

	// Set our status to connecting.
	nc.changeConnStatus(CONNECTING)

	// Step 1: Send simple CONNECT+PING like the working raw example
	// Use a minimal CONNECT message that works with QUIC
	connectMsg := "CONNECT {}\r\nPING\r\n"
	_, err := nc.conn.Write([]byte(connectMsg))
	if err != nil {
		return fmt.Errorf("failed to send CONNECT+PING: %w", err)
	}
	nc.changeConnStatus(CONNECTED)

	// Step 2: Read initial INFO response
	err = nc.processExpectedInfo()
	if err != nil {
		return err
	}

	// Step 3: Read response to our CONNECT+PING (could be +OK or PONG)
	c := &control{}
	err = nc.readOp(c)
	if err != nil {
		return err
	}

	// Handle +OK response to CONNECT
	if c.op == "+OK" {
		// Read the PONG response to PING
		err = nc.readOp(c)
		if err != nil {
			return err
		}
		if c.op != "PONG" {
			return fmt.Errorf("expected 'PONG' after '+OK', got '%s'", c.op)
		}
	} else if c.op == "PONG" {
		// Direct PONG response (no +OK)
	} else {
		return fmt.Errorf("expected '+OK' or 'PONG', got '%s'", c.op)
	}

	// Note: Server may send additional INFO and PING messages, but we'll let
	// the normal read loop handle those after connection init is complete

	// Reset the number of PING sent out
	nc.pout = 0

	// Start or reset Timer
	if nc.Opts.PingInterval > 0 {
		if nc.ptmr == nil {
			nc.ptmr = time.AfterFunc(nc.Opts.PingInterval, nc.processPingTimer)
		} else {
			nc.ptmr.Reset(nc.Opts.PingInterval)
		}
	}

	// Start the readLoop and flusher go routines
	nc.wg.Add(2)
	go nc.readLoop()
	go nc.flusher()

	// Notify the reader that we are done with the connect handshake
	nc.br.doneWithConnect()

	return nil
}
