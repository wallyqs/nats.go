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

package nats

import (
	"fmt"
	"github.com/nats-io/nats.go/internal/test"
)

// TestClient thing.
func NewTestClient(nc *Conn) *TestClient {
	return &TestClient{&testClient{nc}}
}

type TestClient struct {
	// Avoid others attempting to implement this interface
	// thus breaking compatibility.
	// getClient() internal_test.TestClient
	itc *testClient
}

func (tc *TestClient) getClient() internal_test.TestClient {
	fmt.Println("calling private!")
	return tc.itc
}

type testClient struct {
	nc *Conn
}

func (*testClient) SetConnectionStatus(int) {
	fmt.Println("called internal!")
}

// func (*testClient) SetConnectionStatus(status int) {
// 	fmt.Println("AAAAAAAAAAAAAAAAAAAAA")
// }
