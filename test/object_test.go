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
	"path"
	"path/filepath"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestObjectBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdown(s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	err := js.CreateObjectStore(&nats.ObjectConfig{Name: "OBJS"})
	expectOk(t, err)

	// Create ~16MB object.
	blob := make([]byte, 16*1024*1024+22)
	rand.Read(blob)

	err = js.PutObject("OBJS", &nats.ObjectMeta{Name: "BLOB"}, bytes.NewReader(blob))
	expectOk(t, err)

	// Make sure the stream is sealed.
	err = js.SealObjectStore("OBJS")
	expectOk(t, err)
	si, err := js.StreamInfo("OBJ_OBJS")
	expectOk(t, err)
	if !si.Config.Sealed {
		t.Fatalf("Expected the object stream to be sealed, got %+v", si)
	}

	// Check simple errors.
	_, err = js.GetObject("OBJS", "FOO")
	expectErr(t, err)

	// Now get the object back.
	result, err := js.GetObject("OBJS", "BLOB")
	expectOk(t, err)
	expectOk(t, result.Error())
	defer result.Close()

	// Check info stuff.
	info, err := result.Info()
	expectOk(t, err)
	if info.Size != uint64(len(blob)) {
		t.Fatalf("Size does not match, %d vs %d", info.Size, len(blob))
	}

	// Check result.
	copy, err := ioutil.ReadAll(result)
	expectOk(t, err)
	if !bytes.Equal(copy, blob) {
		t.Fatalf("Result not the same")
	}
	// Test delete.
	err = js.DeleteObjectStore("OBJS")
	expectOk(t, err)
	_, err = js.GetObject("OBJS", "BLOB")
	expectErr(t, err, nats.ErrStreamNotFound)
}

func TestObjectFileBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdown(s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	err := js.CreateObjectStore(&nats.ObjectConfig{Name: "FILES"})
	expectOk(t, err)

	// Create ~8MB object.
	blob := make([]byte, 8*1024*1024+33)
	rand.Read(blob)

	tmpFile, err := ioutil.TempFile("", "objfile")
	expectOk(t, err)
	defer os.Remove(tmpFile.Name()) // clean up
	err = ioutil.WriteFile(tmpFile.Name(), blob, 0600)
	expectOk(t, err)

	err = js.PutFile("FILES", tmpFile.Name())
	expectOk(t, err)

	tmpResult, err := ioutil.TempFile("", "objfileresult")
	expectOk(t, err)
	defer os.Remove(tmpResult.Name()) // clean up

	err = js.GetFile("FILES", tmpFile.Name(), tmpResult.Name())
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

func TestObjectMulti(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdown(s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	on := "TEST_FILES"
	err := js.CreateObjectStore(&nats.ObjectConfig{Name: on})
	expectOk(t, err)

	numFiles := 0
	fis, _ := ioutil.ReadDir(".")
	for _, fi := range fis {
		fn := fi.Name()
		// Just grab clean test files.
		if filepath.Ext(fn) != ".go" || fn[0] == '.' || fn[0] == '#' {
			continue
		}
		err = js.PutFile(on, fn)
		expectOk(t, err)
		numFiles++
	}
	expectOk(t, js.SealObjectStore(on))

	_, err = js.StreamInfo("OBJ_TEST_FILES")
	expectOk(t, err)

	result, err := js.GetObject(on, "object_test.go")
	expectOk(t, err)
	expectOk(t, result.Error())
	defer result.Close()

	_, err = result.Info()
	expectOk(t, err)

	copy, err := ioutil.ReadAll(result)
	expectOk(t, err)

	orig, err := ioutil.ReadFile(path.Join(".", "object_test.go"))
	expectOk(t, err)

	if !bytes.Equal(orig, copy) {
		t.Fatalf("Files did not match")
	}
}

func TestObjectMultiWithDelete(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdown(s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	err := js.CreateObjectStore(&nats.ObjectConfig{Name: "2OD"})
	expectOk(t, err)

	pa := bytes.Repeat([]byte("A"), 2_000_000)
	pb := bytes.Repeat([]byte("B"), 3_000_000)

	err = js.PutObject("2OD", &nats.ObjectMeta{Name: "A"}, bytes.NewReader(pa))
	expectOk(t, err)

	// Hold onto this so we can make sure DeleteObject clears all messages, chunks and meta.
	si, err := js.StreamInfo("OBJ_2OD")
	expectOk(t, err)

	err = js.PutObject("2OD", &nats.ObjectMeta{Name: "B"}, bytes.NewReader(pb))
	expectOk(t, err)

	result, err := js.GetObject("2OD", "B")
	expectOk(t, err)
	expectOk(t, result.Error())
	defer result.Close()

	pb2, err := ioutil.ReadAll(result)
	expectOk(t, err)

	if !bytes.Equal(pb, pb2) {
		t.Fatalf("Did not retrieve same object")
	}

	// Now delete B
	err = js.DeleteObject("2OD", "B")
	expectOk(t, err)

	siad, err := js.StreamInfo("OBJ_2OD")
	expectOk(t, err)
	if siad.State.Msgs != si.State.Msgs {
		t.Fatalf("Expected to have %d msgs after delete, got %d", siad.State.Msgs, si.State.Msgs)
	}
}

func TestObjectNames(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdown(s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	err := js.CreateObjectStore(&nats.ObjectConfig{Name: "OBJS"})
	expectOk(t, err)

	// Create ~1K object.
	blob := make([]byte, 1024)
	rand.Read(blob)
	r := bytes.NewReader(blob)

	// Test filename like naming.
	err = js.PutObject("OBJS", &nats.ObjectMeta{Name: "BLOB.txt"}, r)
	expectOk(t, err)
	// Spaces ok
	err = js.PutObject("OBJS", &nats.ObjectMeta{Name: "foo bar"}, r)
	expectOk(t, err)

	// Errors
	err = js.PutObject("OBJS", &nats.ObjectMeta{Name: "*"}, r)
	expectErr(t, err)
	err = js.PutObject("OBJS", &nats.ObjectMeta{Name: ">"}, r)
	expectErr(t, err)
	err = js.PutObject("OBJS", &nats.ObjectMeta{Name: ""}, r)
	expectErr(t, err)
	err = js.PutObject("OBJS", &nats.ObjectMeta{Name: "\t"}, r)
	expectErr(t, err)
	err = js.PutObject("OBJS", &nats.ObjectMeta{Name: "\n"}, r)
	expectErr(t, err)
}
