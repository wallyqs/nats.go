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

package pubasync

import (
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// PUB-6: PublishMsgAsync runs newAsyncReply (publish.go:362, its own lock
// hold) and registerPAF (publish.go:367, another lock hold) separately. A
// CleanupPublisher in between unsubscribes the reply subscription and
// removes the status listener; registerPAF then registered the future under
// the stale js.publisher.replyPrefix (cleanup kept it, jetstream.go:1317-1331),
// the publish went out with a reply subject nobody listens on, and the
// future was never resolved (no ack timeout) -- PublishAsyncComplete never
// closed either. Fixed: cleanup clears the prefix and registerPAF re-creates
// the subscription when it sees that. The window is a few instructions
// wide, so this is a stress test: it races PublishAsync against
// CleanupPublisher and fails if a future is orphaned (before the fix it did
// within a few hundred rounds); TestPUB2CleanupDoesNotWakeStalledPublisher
// opens the same window deterministically.
// Model: PUB6_cleanup_orphan_bug.cfg (NoOrphanedFuture).
func TestPUB6CleanupRaceOrphansFuture(t *testing.T) {
	e := newEnv(t, nil)
	e.warmUp()
	const rounds = 20000
	deadline := time.Now().Add(60 * time.Second)
	for i := 0; i < rounds && time.Now().Before(deadline); i++ {
		var (
			wg    sync.WaitGroup
			paf   jetstream.PubAckFuture
			perr  error
			start = make(chan struct{})
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			paf, perr = e.js.PublishAsync("s.race", nil)
		}()
		go func() {
			defer wg.Done()
			<-start
			e.js.CleanupPublisher()
		}()
		close(start)
		wg.Wait()
		if perr != nil {
			continue
		}
		select {
		case <-paf.Ok():
		case <-paf.Err():
		case <-time.After(2 * time.Second):
			t.Fatalf("PUB-6: round %d: future registered across CleanupPublisher never resolved (pending=%d)", i, e.js.PublishAsyncPending())
		}
	}
}
