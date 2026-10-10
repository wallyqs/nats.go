// Copyright 2026 The NATS Authors
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

//go:build internal_testing

package kvwatch

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	zizek "github.com/wallyqs/zizek.go"
)

// dropNext is the model's NetDrop: the next JetStream message delivered
// for key k on the watchers' connection is lost before processMsg. The
// ordered consumer notices the gap on the next message or heartbeat and
// resets (so settling may take a heartbeat interval or two).
func (s *kvMachine) dropNext(tc *zizek.TestCase) error {
	k, err := s.drawKey(tc)
	if err != nil {
		return err
	}
	var n atomic.Int32
	s.wnc.AddMsgFilter(s.subject(k), func(m *nats.Msg) *nats.Msg {
		if m.Reply != "" && n.Add(1) == 1 {
			return nil
		}
		return m
	})
	tc.Note(fmt.Sprintf("dropNext %s (armed)", k))
	return nil
}

// TestKVWatchModelFaults adds message loss to TestKVWatchModel. It used to
// find KV-1 (stale revision / premature marker after a reset in the initial
// phase) and KV-2 (UpdatesOnly / ResumeFromRevision replay from seq 1).
// A lost message that is the last one is only noticed at the next idle
// heartbeat (5s), so examples are slow: 40 of them take about 5 minutes.
func TestKVWatchModelFaults(t *testing.T) {
	f := newFixture(t)
	extra := []zizek.Rule[kvMachine]{{Name: "dropNext", Apply: (*kvMachine).dropNext}}
	err := zizek.Check(f.property(extra, 30, 15*time.Second),
		zizek.Config{Examples: 40, Seed: 3, MaxShrinks: 40})
	f.logStats()
	if err != nil {
		t.Fatal(err)
	}
}
