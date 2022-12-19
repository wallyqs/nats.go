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

import "github.com/nats-io/nats.go/internal/test"

// TestClient is a helper function used internally for testing
// across other nats packages.
func TestClient(nc *Conn) TC {
	return &testClient{nc, nil}
}

// TC is an interface internally used for testing across other packages.
type TC interface {
	// Ensures that no one can implement or use this interface
	// so that it cannot be depended upon outside this package.
	private()
}

// testClient implements the TC interface and interfaces from
// the internal/test package that can be used to change behavior
// from the client.
type testClient struct {
	nc *Conn
	internal_test.TC
}

// private implements the TC interface.
func (tc *testClient) private() {}

// InternalTestEngine returns an interface that can be used for changing
// the internal state of the client from other packages.
func (tc *testClient) InternalTestClient() internal_test.InternalTestClient {
	return &internalTestClient{tc, nil}
}

// internalTestEngine implements the internal testing interface.
type internalTestClient struct {
	tc *testClient
	internal_test.InternalTestClient
}

// SetConnectionStatus overrides the status of the connection.
func (e *internalTestClient) SetConnectionStatus(status int) {
	nc := e.tc.nc
	nc.mu.Lock()
	nc.status = Status(status)
	nc.mu.Unlock()
}

// ConnectionStatus gets the status of the connection.
func (e *internalTestClient) ConnectionStatus() int {
	nc := e.tc.nc
	nc.mu.Lock()
	status := int(nc.status)
	nc.mu.Unlock()
	return status
}
