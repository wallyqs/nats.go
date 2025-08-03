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
	"net/url"
	"testing"
)

func TestQuicSchemeDetection(t *testing.T) {
	testCases := []struct {
		urlStr   string
		expected bool
	}{
		{"quic://localhost:4222", true},
		{"quic://example.com:1234", true},
		{"nats://localhost:4222", false},
		{"ws://localhost:8080", false},
		{"wss://localhost:443", false},
		{"tls://localhost:4222", false},
	}

	for _, tc := range testCases {
		t.Run(tc.urlStr, func(t *testing.T) {
			u, err := url.Parse(tc.urlStr)
			if err != nil {
				t.Fatalf("Failed to parse URL %s: %v", tc.urlStr, err)
			}
			result := isQuicScheme(u)
			if result != tc.expected {
				t.Errorf("isQuicScheme(%s) = %v, want %v", tc.urlStr, result, tc.expected)
			}
		})
	}
}

func TestQuicConnScheme(t *testing.T) {
	nc := &Conn{quic: true}
	scheme := nc.connScheme()
	if scheme != quicScheme {
		t.Errorf("connScheme() for QUIC connection = %s, want %s", scheme, quicScheme)
	}

	// Test that QUIC takes precedence over secure
	nc.Opts.Secure = true
	scheme = nc.connScheme()
	if scheme != quicScheme {
		t.Errorf("connScheme() for QUIC+Secure connection = %s, want %s", scheme, quicScheme)
	}
}

func TestQuicPortAssignment(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{"quic://localhost", "quic://localhost:4222"},
		{"quic://localhost:", "quic://localhost:4222"},
		{"quic://localhost:1234", "quic://localhost:1234"},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			opts := GetDefaultOptions()
			nc := &Conn{Opts: opts, quic: true}
			nc.srvPool = make([]*srv, 0)
			nc.urls = make(map[string]struct{})

			err := nc.addURLToPool(tc.input, false, false)
			if err != nil {
				t.Fatalf("addURLToPool failed: %v", err)
			}

			if len(nc.srvPool) == 0 {
				t.Fatal("Expected server to be added to pool")
			}

			actualURL := nc.srvPool[0].url.String()
			if actualURL != tc.expected {
				t.Errorf("addURLToPool(%s) resulted in %s, want %s", tc.input, actualURL, tc.expected)
			}
		})
	}
}

func TestQuicConnectionAttempt(t *testing.T) {
	// Test that QUIC connection setup works (without actually connecting)
	opts := GetDefaultOptions()
	opts.Servers = []string{"quic://localhost:4222"}

	// Create connection without actually connecting
	nc := &Conn{Opts: opts}
	err := nc.setupServerPool()
	if err != nil {
		t.Fatalf("setupServerPool failed: %v", err)
	}

	if !nc.quic {
		t.Error("Expected QUIC flag to be set")
	}

	if len(nc.srvPool) == 0 {
		t.Fatal("Expected server to be added to pool")
	}

	if nc.srvPool[0].url.Scheme != quicScheme {
		t.Errorf("Expected scheme to be %s, got %s", quicScheme, nc.srvPool[0].url.Scheme)
	}
}
