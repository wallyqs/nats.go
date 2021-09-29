// Copyright 2021 The NATS Authors
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

package test

import (
	"bytes"
	"crypto/rand"
	"io/ioutil"
	"os"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestObjectBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdown(s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	// Create ~16MB object.
	blob := make([]byte, 16*1024*1024+22)
	rand.Read(blob)

	err := js.PutObject(&nats.ObjectConfig{Name: "BLOB"}, bytes.NewReader(blob))
	expectOk(t, err)

	si, err := js.StreamInfo("OBJ_BLOB")
	expectOk(t, err)
	// Make sure the stream is sealed.
	if si.State.Msgs != uint64(si.Config.MaxMsgs) || si.Config.MaxMsgSize != 1 {
		t.Fatalf("Expected the object stream to be sealed, got %+v", si)
	}

	// Check simple errors.
	_, err = js.GetObject("FOO")
	expectErr(t, err, nats.ErrStreamNotFound)

	// Now get the object back.
	result, err := js.GetObject("BLOB")
	expectOk(t, err)
	defer result.Close()

	// Check result.
	copy, err := ioutil.ReadAll(result)
	expectOk(t, err)
	if !bytes.Equal(copy, blob) {
		t.Fatalf("Result not the same")
	}
	// Test delete.
	err = js.DeleteObject("BLOB")
	expectOk(t, err)
	_, err = js.GetObject("BLOB")
	expectErr(t, err, nats.ErrStreamNotFound)
}

func TestObjectFileBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdown(s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	// Create ~8MB object.
	blob := make([]byte, 8*1024*1024+33)
	rand.Read(blob)

	tmpFile, err := ioutil.TempFile("", "objfile")
	expectOk(t, err)
	defer os.Remove(tmpFile.Name()) // clean up
	err = ioutil.WriteFile(tmpFile.Name(), blob, 0600)
	expectOk(t, err)

	err = js.PutFile(&nats.ObjectConfig{Name: "FILE"}, tmpFile.Name())
	expectOk(t, err)

	tmpResult, err := ioutil.TempFile("", "objfileresult")
	expectOk(t, err)
	defer os.Remove(tmpResult.Name()) // clean up

	err = js.GetFile("FILE", tmpResult.Name())
	expectOk(t, err)

	// Make sure they are the same.
	original, err := ioutil.ReadFile(tmpFile.Name())
	expectOk(t, err)

	restored, err := ioutil.ReadFile(tmpResult.Name())
	expectOk(t, err)

	if !bytes.Equal(original, restored) {
		t.Fatalf("Files did not match")
	}
}
