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

type quicConn struct {
	quic.Connection
	stream quic.Stream
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

func (qc *quicConn) LocalAddr() net.Addr {
	return qc.Connection.LocalAddr()
}

func (qc *quicConn) RemoteAddr() net.Addr {
	return qc.Connection.RemoteAddr()
}

func (qc *quicConn) SetDeadline(t time.Time) error {
	qc.stream.SetDeadline(t)
	return nil
}

func (qc *quicConn) SetReadDeadline(t time.Time) error {
	qc.stream.SetReadDeadline(t)
	return nil
}

func (qc *quicConn) SetWriteDeadline(t time.Time) error {
	qc.stream.SetWriteDeadline(t)
	return nil
}

// quicInitHandshake establishes a QUIC connection
func (nc *Conn) quicInitHandshake(u *url.URL) error {
	tlsConf := &tls.Config{
		ServerName:         u.Hostname(),
		InsecureSkipVerify: true, // Default to true for QUIC, can be overridden
		NextProtos:         []string{"nats"},
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
	}

	ctx := context.Background()
	if nc.Opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, nc.Opts.Timeout)
		defer cancel()
	}

	conn, err := quic.DialAddr(ctx, u.Host, tlsConf, nil)
	if err != nil {
		return fmt.Errorf("failed to establish QUIC connection: %w", err)
	}

	// Open a bidirectional stream for NATS communication
	// Use background context for stream opening to avoid timeout issues
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		conn.CloseWithError(0, "failed to open stream")
		return fmt.Errorf("failed to open QUIC stream: %w", err)
	}

	// Wrap the QUIC connection and stream
	nc.conn = &quicConn{
		Connection: conn,
		stream:     stream,
	}

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
