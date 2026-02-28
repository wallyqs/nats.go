// Copyright 2012-2025 The NATS Authors
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
	"context"
	"crypto/tls"
	"flag"
	"log"
	"net"
	"net/http/httptrace"
	"os"
	"runtime"
	"time"

	"github.com/nats-io/nats.go"
)

// NOTE: Can test with demo TLS server.
// nats-sub-httptrace -s tls://demo.nats.io:4443 <subject>

func usage() {
	log.Printf("Usage: nats-sub-httptrace [-s server] [-creds file] [-t] <subject>\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func printMsg(m *nats.Msg, i int) {
	log.Printf("[#%d] Received on [%s]: '%s'", i, m.Subject, string(m.Data))
}

// traceDialer wraps a *net.Dialer and uses httptrace to log connection details
// such as DNS resolution results and the remote server IP address after TLS dial.
type traceDialer struct {
	dialer *net.Dialer
}

func (td *traceDialer) Dial(network, address string) (net.Conn, error) {
	trace := &httptrace.ClientTrace{
		DNSDone: func(info httptrace.DNSDoneInfo) {
			if info.Err != nil {
				log.Printf("[httptrace] DNS error: %v", info.Err)
				return
			}
			for _, addr := range info.Addrs {
				log.Printf("[httptrace] DNS resolved: %s", addr.String())
			}
		},
		ConnectDone: func(network, addr string, err error) {
			if err != nil {
				log.Printf("[httptrace] TCP connect to %s failed: %v", addr, err)
				return
			}
			log.Printf("[httptrace] TCP connected to %s", addr)
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			if err != nil {
				log.Printf("[httptrace] TLS handshake failed: %v", err)
				return
			}
			log.Printf("[httptrace] TLS handshake complete: version=0x%04x, server=%s",
				state.Version, state.ServerName)
		},
	}
	ctx := httptrace.WithClientTrace(context.Background(), trace)
	return td.dialer.DialContext(ctx, network, address)
}

func main() {
	var urls = flag.String("s", "tls://demo.nats.io:4443", "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var showTime = flag.Bool("t", false, "Display timestamps")
	var showHelp = flag.Bool("h", false, "Show help message")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	args := flag.Args()
	if len(args) != 1 {
		showUsageAndExit(1)
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS httptrace Subscriber")}
	opts = setupConnOptions(opts)

	// Use UserCredentials
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	// Set up the custom dialer with httptrace to log the server IP.
	opts = append(opts, nats.SetCustomDialer(&traceDialer{
		dialer: &net.Dialer{
			Timeout: 10 * time.Second,
		},
	}))

	// Enable TLS.
	opts = append(opts, nats.Secure())

	// Connect to NATS
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Connected to %s (server IP: %s)", nc.ConnectedUrl(), nc.ConnectedAddr())

	subj, i := args[0], 0

	nc.Subscribe(subj, func(msg *nats.Msg) {
		i += 1
		printMsg(msg, i)
	})
	nc.Flush()

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening on [%s]", subj)
	if *showTime {
		log.SetFlags(log.LstdFlags)
	}

	runtime.Goexit()
}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		log.Printf("Disconnected due to: %s, will attempt reconnects for %.0fm", err, totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Fatalf("Exiting: %v", nc.LastError())
	}))
	return opts
}
