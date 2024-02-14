// Copyright 2012-2022 The NATS Authors
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
	"flag"
	"log"
	"net"
	"os"
	"runtime"
	"time"

	"net/http/httptrace"

	"github.com/nats-io/nats.go"
)

type customDialer struct {
	ctx             context.Context
	nc              *nats.Conn
	connectTimeout  time.Duration
	connectTimeWait time.Duration
}

func (cd *customDialer) Dial(network, address string) (net.Conn, error) {
	d := &net.Dialer{
		FallbackDelay: -1,
	}
	start := time.Now()
	trace := &httptrace.ClientTrace{
		ConnectStart: func(network, addr string) {
			log.Println("Attempting to connect to server at", addr, time.Since(start))
		},
		DNSStart: func(httptrace.DNSStartInfo) {
			log.Println("DNS Lookup", time.Since(start))
		},
		DNSDone:  func(httptrace.DNSDoneInfo) {
			log.Println("DNS DONE", time.Since(start))
		},
		ConnectDone: func(network, addr string, err error) {
			if err == nil {
				log.Println(time.Now(), "Established TCP connection", time.Since(start))
			} else {
				log.Println("Failed connecting to", addr, time.Since(start))
			}
		},
	}
	ctx := httptrace.WithClientTrace(cd.ctx, trace)
	ctx, cancel := context.WithTimeout(ctx, cd.connectTimeout)
	defer cancel()
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			if conn, err := d.DialContext(ctx, network, address); err == nil {
				log.Println("Connected to NATS successfully")
				return conn, nil
			} else {
				log.Println(err, ctx.Err())
				time.Sleep(cd.connectTimeWait)
			}
		}
	}
}

// NOTE: Can test with demo servers.
// nats-sub -s demo.nats.io <subject>

func usage() {
	log.Printf("Usage: nats-sub [-s server] [-creds file] [-nkey file] [-tlscert file] [-tlskey file] [-tlscacert file] [-t] <subject>\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func printMsg(m *nats.Msg, i int) {
	log.Printf("[#%d] Received on [%s]: '%s'", i, m.Subject, string(m.Data))
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var nkeyFile = flag.String("nkey", "", "NKey Seed File")
	var tlsClientCert = flag.String("tlscert", "", "TLS client certificate file")
	var tlsClientKey = flag.String("tlskey", "", "Private key file for client certificate")
	var tlsCACert = flag.String("tlscacert", "", "CA certificate to verify peer against")
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

	// -----------------
	// Parent context cancels connecting/reconnecting altogether.
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	var err error
	var nc *nats.Conn
	// cd := &customDialer{
	// 	ctx:             ctx,
	// 	nc:              nc,
	// 	connectTimeout:  14 * time.Millisecond,
	// 	connectTimeWait: 1 * time.Second,
	// }
	// -----------------

	// Connect Options.
	// opts := []nats.Option{nats.Name("NATS Sample Subscriber"), nats.Timeout(15 * time.Millisecond), nats.SetCustomDialer(cd)}
	// opts := []nats.Option{nats.Name("NATS Sample Subscriber"), nats.SetCustomDialer(cd)}
	opts := []nats.Option{nats.Name("NATS Sample Subscriber"), nats.Timeout(30 * time.Millisecond)}
	opts = setupConnOptions(opts)

	if *userCreds != "" && *nkeyFile != "" {
		log.Fatal("specify -seed or -creds")
	}

	// Use UserCredentials
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	// Use TLS client authentication
	if *tlsClientCert != "" && *tlsClientKey != "" {
		opts = append(opts, nats.ClientCert(*tlsClientCert, *tlsClientKey))
	}

	// Use specific CA certificate
	if *tlsCACert != "" {
		opts = append(opts, nats.RootCAs(*tlsCACert))
	}

	// Use Nkey authentication.
	if *nkeyFile != "" {
		opt, err := nats.NkeyOptionFromSeed(*nkeyFile)
		if err != nil {
			log.Fatal(err)
		}
		opts = append(opts, opt)
	}

	// Connect to NATS
	nc, err = nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}

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
		log.Printf("Disconnected due to:%s, will attempt reconnects for %.0fm", err, totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Fatalf("Exiting: %v", nc.LastError())
	}))
	return opts
}
