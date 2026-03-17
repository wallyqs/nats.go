// Copyright 2012-2024 The NATS Authors
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

package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"

	"github.com/nats-io/nats.go"
)

// proxyDialer dials through an HTTP CONNECT proxy.
type proxyDialer struct {
	proxyURL *url.URL
}

func (d *proxyDialer) Dial(network, address string) (net.Conn, error) {
	conn, err := net.Dial("tcp", d.proxyURL.Host)
	if err != nil {
		return nil, fmt.Errorf("dial proxy: %w", err)
	}

	hdr := make(http.Header)
	if d.proxyURL.User != nil {
		password, _ := d.proxyURL.User.Password()
		creds := d.proxyURL.User.Username() + ":" + password
		hdr.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(creds)))
	}

	connectReq := &http.Request{
		Method: "CONNECT",
		URL:    &url.URL{Opaque: address},
		Host:   address,
		Header: hdr,
	}
	if err := connectReq.Write(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write CONNECT: %w", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), connectReq)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read CONNECT response: %w", err)
	}
	if resp.StatusCode != 200 {
		conn.Close()
		return nil, fmt.Errorf("proxy CONNECT failed: %s", resp.Status)
	}

	return conn, nil
}

func main() {
	opts := []nats.Option{nats.Name("NATS Demo Publisher")}

	// Use HTTP CONNECT proxy if https_proxy is set.
	if proxyEnv := os.Getenv("https_proxy"); proxyEnv != "" {
		proxyURL, err := url.Parse(proxyEnv)
		if err != nil {
			log.Fatalf("invalid https_proxy: %v", err)
		}
		log.Printf("Using proxy %s", proxyURL.Host)
		opts = append(opts, nats.SetCustomDialer(&proxyDialer{proxyURL: proxyURL}))
	}

	// Connect to the demo NATS server.
	nc, err := nats.Connect("demo.nats.io", opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// Publish a message to the "wallyqs" subject.
	msg := "Hello from nats.go!"
	if err := nc.Publish("wallyqs", []byte(msg)); err != nil {
		log.Fatal(err)
	}

	// Make sure the message is sent before closing.
	nc.Flush()

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Published [wallyqs] : '%s'\n", msg)
}
