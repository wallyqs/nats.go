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
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestKeyValueBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdown(s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST"})
	expectOk(t, err)

	if kv.Bucket() != "TEST" {
		t.Fatalf("Expected bucket name to be %q, got %q", "TEST", kv.Bucket())
	}

	// Simple Put
	r, err := kv.Put("name", []byte("derek"))
	expectOk(t, err)
	if r != 1 {
		t.Fatalf("Expected 1 for the revision, got %d", r)
	}
	// Simple Get
	e, err := kv.Get("name")
	expectOk(t, err)
	if string(e.Value()) != "derek" {
		t.Fatalf("Got wrong value: %q vs %q", e.Value(), "derek")
	}
	if e.Revision() != 1 {
		t.Fatalf("Expected 1 for the revision, got %d", e.Revision())
	}

	// Delete
	err = kv.Delete("name")
	expectOk(t, err)
	_, err = kv.Get("name")
	expectErr(t, err)
	r, err = kv.Create("name", []byte("derek"))
	expectOk(t, err)
	if r != 3 {
		t.Fatalf("Expected 3 for the revision, got %d", r)
	}

	// Conditional Updates.
	r, err = kv.Update("name", []byte("rip"), 3)
	expectOk(t, err)
	_, err = kv.Update("name", []byte("ik"), 3)
	expectErr(t, err)
	_, err = kv.Update("name", []byte("ik"), r)
	expectOk(t, err)
	r, err = kv.Create("age", []byte("22"))
	expectOk(t, err)
	_, err = kv.Update("age", []byte("33"), r)
	expectOk(t, err)
}

func TestKeyValueList(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdown(s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "LIST", History: 10})
	expectOk(t, err)

	for i := 0; i < 50; i++ {
		age := strconv.FormatUint(uint64(i+22), 10)
		_, err := kv.Put("age", []byte(age))
		expectOk(t, err)
	}

	vl, err := kv.List("age")
	expectOk(t, err)

	if len(vl) != 10 {
		t.Fatalf("Expected %d values, got %d", 10, len(vl))
	}
	for i, v := range vl {
		if v.Key() != "age" {
			t.Fatalf("Expected key of %q, got %q", "age", v.Key())
		}
		if v.Revision() != uint64(i+41) {
			// History of 10, sent 50..
			t.Fatalf("Expected revision of %d, got %d", i+41, v.Revision())
		}
		age, err := strconv.Atoi(string(v.Value()))
		expectOk(t, err)
		if age != i+62 {
			t.Fatalf("Expected data value of %d, got %d", i+22, age)
		}
	}
}

func TestKeyValueWatch(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdown(s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH"})
	expectOk(t, err)

	updates := make(chan nats.KeyValueEntry, 32)
	sub, err := kv.WatchAll(func(v nats.KeyValueEntry) {
		updates <- v
	})
	expectOk(t, err)
	defer sub.Unsubscribe()

	expectUpdate := func(key, value string, revision uint64) {
		t.Helper()
		select {
		case v := <-updates:
			if v.Key() != key || string(v.Value()) != value || v.Revision() != revision {
				t.Fatalf("Did not get expected: %+v vs %q %q %d", v, key, value, revision)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive an update like expected")
		}
	}
	expectDelete := func(key string, revision uint64) {
		t.Helper()
		select {
		case v := <-updates:
			if v.Operation() != nats.KeyValueDelete {
				t.Fatalf("Expected a delete operation but got %+v", v)
			}
			if v.Revision() != revision {
				t.Fatalf("Did not get expected revision: %d vs %d", revision, v.Revision())
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive an update like expected")
		}
	}

	kv.Create("name", []byte("derek"))
	expectUpdate("name", "derek", 1)
	kv.Put("name", []byte("rip"))
	expectUpdate("name", "rip", 2)
	kv.Put("name", []byte("ik"))
	expectUpdate("name", "ik", 3)
	kv.Put("age", []byte("22"))
	expectUpdate("age", "22", 4)
	kv.Put("age", []byte("33"))
	expectUpdate("age", "33", 5)
	kv.Delete("age")
	expectDelete("age", 6)

	// Stop first watcher.
	sub.Unsubscribe()

	// Now try wildcard matching and make sure we only get last value when starting.
	kv.Put("t.name", []byte("rip"))
	kv.Put("t.name", []byte("ik"))
	kv.Put("t.age", []byte("22"))
	kv.Put("t.age", []byte("44"))

	sub, err = kv.Watch("t.*", func(v nats.KeyValueEntry) {
		updates <- v
	})
	expectOk(t, err)
	defer sub.Unsubscribe()

	expectUpdate("t.name", "ik", 8)
	expectUpdate("t.age", "44", 10)
}

func TestKeyValueBindStore(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdown(s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH"})
	expectOk(t, err)

	// Now bind to it..
	_, err = js.KeyValue("WATCH")
	expectOk(t, err)

	// Make sure we can't bind to a non-kv style stream.
	// We have some protection with stream name prefix.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "KV_TEST",
		Subjects: []string{"foo"},
	})
	expectOk(t, err)

	_, err = js.KeyValue("TEST")
	expectErr(t, err)
	if err != nats.ErrBadBucket {
		t.Fatalf("Expected %v but got %v", nats.ErrBadBucket, err)
	}
}

func TestKeyValueDeleteStore(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdown(s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH"})
	expectOk(t, err)

	err = js.DeleteKeyValue("WATCH")
	expectOk(t, err)

	_, err = js.KeyValue("WATCH")
	expectErr(t, err)
}

// Helpers

func client(t *testing.T, s *server.Server) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	return nc
}

func jsClient(t *testing.T, s *server.Server) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc := client(t, s)
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func shutdown(s *server.Server) {
	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}
	s.Shutdown()
}

func expectOk(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func expectErr(t *testing.T, err error, expected ...error) {
	t.Helper()
	if err == nil {
		t.Fatalf("Expected error but got none")
	}
	if len(expected) == 0 {
		return
	}
	for _, e := range expected {
		if err == e || strings.Contains(e.Error(), err.Error()) {
			return
		}
	}
	t.Fatalf("Expected one of %+v, got '%v'", expected, err)
}
