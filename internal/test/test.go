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

package internal_test

// InternalTestClient includes special behavior to support integration tests
// that depend on internals of the client.
type InternalTestClient interface {
	ConnectionStatus() int
	SetConnectionStatus(int)
	// Ensures that users can not implement this interface unless
	// the interface type is embedded, so only code that is able
	// to reach the internal package.
	private()
}

// TC is the InternalTestClient interface used to access private access
// from the package.
type TC interface {
	InternalTestClient() InternalTestClient
	private()
}
