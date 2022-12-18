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

// TestClient is a helper function used for internal testing,
// it is not helpful to use outside the nats module.
func TestClient(nc *Conn) TC {
	return &testClient{nc, nil}
}

// TC is an interface internally used for testing.
type TC interface {
	// IsTestClient() bool
	// Ensures that no one can implement or use this interface
	// outside of the current package unless type is embedded.
	private()
}

// testClient implements the TC interface.
type testClient struct {
	nc *Conn
	internal_test.Client
}

func (tc *testClient) private() {}

func (tc *testClient) InternalTestEngine() internal_test.Engine {
	return &internalTestEngine{tc, nil}
}

// func (tc *testClient) IsTestClient() bool {
// 	return true
// }

type internalTestEngine struct {
	tc *testClient
	internal_test.Engine
}

// func (e *internalTestEngine) private() {}

// SetConnectionStatus overrides the status of the connection.
func (e *internalTestEngine) SetConnectionStatus(status int) {
	nc := e.tc.nc
	nc.mu.Lock()
	nc.status = Status(status)
	nc.mu.Unlock()
}
