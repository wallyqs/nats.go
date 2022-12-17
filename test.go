// Copyright 2022 The NATS Authors
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

import it "github.com/nats-io/nats.go/internal/test"

// TestClient is a helper function used in internal testing from
// the nats module, it cannot be used outside of this repo.
func TestClient(nc *Conn, _ it.TC) it.TestClient {
	return &testClient{nc: nc}
}

// testClient implements the it.TestClient interface.
type testClient struct {
	nc *Conn
	it.TestClient
}

// SetConnectionStatus overrides the status of the connection.
func (tc *testClient) SetConnectionStatus(status int) {
	tc.nc.mu.Lock()
	tc.nc.status = Status(status)
	tc.nc.mu.Unlock()
}
