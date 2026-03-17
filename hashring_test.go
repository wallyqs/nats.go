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

import (
	"fmt"
	"math"
	"testing"
)

func TestHashRingBasic(t *testing.T) {
	hr := NewHashRing()

	if hr.Size() != 0 {
		t.Fatalf("Expected size 0, got %d", hr.Size())
	}

	if _, err := hr.Get("key"); err == nil {
		t.Fatal("Expected error on empty ring")
	}

	if err := hr.Add("server1"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := hr.Add("server2"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := hr.Add("server3"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if hr.Size() != 3 {
		t.Fatalf("Expected size 3, got %d", hr.Size())
	}

	member, err := hr.Get("my-key")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if member == "" {
		t.Fatal("Expected non-empty member")
	}
}

func TestHashRingConsistency(t *testing.T) {
	hr := NewHashRing()
	hr.Add("server1")
	hr.Add("server2")
	hr.Add("server3")

	// Same key should always return the same member.
	results := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		member, _ := hr.Get(key)
		results[key] = member
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		member, _ := hr.Get(key)
		if member != results[key] {
			t.Fatalf("Inconsistent result for %s: got %s, expected %s", key, member, results[key])
		}
	}
}

func TestHashRingMinimalRemapping(t *testing.T) {
	hr := NewHashRing()
	hr.Add("server1")
	hr.Add("server2")
	hr.Add("server3")

	// Record assignments.
	numKeys := 10000
	before := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		before[key], _ = hr.Get(key)
	}

	// Add a 4th server.
	hr.Add("server4")

	// Check how many keys moved.
	moved := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		after, _ := hr.Get(key)
		if after != before[key] {
			moved++
		}
	}

	// With consistent hashing, roughly 1/4 of keys should move when going
	// from 3 to 4 servers. Allow a generous margin.
	expectedFraction := 1.0 / 4.0
	actualFraction := float64(moved) / float64(numKeys)
	if actualFraction > expectedFraction*2 {
		t.Fatalf("Too many keys moved: %d/%d (%.2f%%), expected ~%.2f%%",
			moved, numKeys, actualFraction*100, expectedFraction*100)
	}
}

func TestHashRingRemove(t *testing.T) {
	hr := NewHashRing()
	hr.Add("server1")
	hr.Add("server2")
	hr.Add("server3")

	member, _ := hr.Get("test-key")

	// Remove a different server; key should stay on same member.
	other := ""
	for _, s := range []string{"server1", "server2", "server3"} {
		if s != member {
			other = s
			break
		}
	}

	hr.Remove(other)
	after, _ := hr.Get("test-key")
	if after != member {
		t.Fatalf("Key moved after removing unrelated server: %s -> %s", member, after)
	}

	if hr.Size() != 2 {
		t.Fatalf("Expected size 2, got %d", hr.Size())
	}

	// Remove non-existent member.
	if err := hr.Remove("nonexistent"); err == nil {
		t.Fatal("Expected error removing non-existent member")
	}
}

func TestHashRingWeighted(t *testing.T) {
	hr := NewHashRing()
	hr.AddWeighted("heavy", 5)
	hr.AddWeighted("light", 1)

	counts := map[string]int{"heavy": 0, "light": 0}
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		member, _ := hr.Get(key)
		counts[member]++
	}

	// "heavy" should get roughly 5x more keys than "light".
	ratio := float64(counts["heavy"]) / float64(counts["light"])
	if ratio < 2.5 || ratio > 10.0 {
		t.Fatalf("Weight ratio out of expected range: heavy=%d, light=%d, ratio=%.2f",
			counts["heavy"], counts["light"], ratio)
	}
}

func TestHashRingDistribution(t *testing.T) {
	hr := NewHashRing()
	numServers := 5
	for i := 0; i < numServers; i++ {
		hr.Add(fmt.Sprintf("server%d", i))
	}

	counts := make(map[string]int)
	numKeys := 50000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		member, _ := hr.Get(key)
		counts[member]++
	}

	expected := float64(numKeys) / float64(numServers)
	for member, count := range counts {
		deviation := math.Abs(float64(count)-expected) / expected
		if deviation > 0.25 {
			t.Fatalf("Poor distribution for %s: got %d, expected ~%.0f (%.1f%% deviation)",
				member, count, expected, deviation*100)
		}
	}
}

func TestHashRingGetN(t *testing.T) {
	hr := NewHashRing()
	hr.Add("server1")
	hr.Add("server2")
	hr.Add("server3")

	members, err := hr.GetN("key", 2)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("Expected 2 members, got %d", len(members))
	}
	if members[0] == members[1] {
		t.Fatalf("GetN returned duplicate members: %v", members)
	}

	// Requesting more than available should cap at member count.
	all, _ := hr.GetN("key", 10)
	if len(all) != 3 {
		t.Fatalf("Expected 3 members (capped), got %d", len(all))
	}

	// All should be unique.
	seen := make(map[string]bool)
	for _, m := range all {
		if seen[m] {
			t.Fatalf("Duplicate member in GetN result: %s", m)
		}
		seen[m] = true
	}
}

func TestHashRingDuplicateMember(t *testing.T) {
	hr := NewHashRing()
	hr.Add("server1")

	if err := hr.Add("server1"); err == nil {
		t.Fatal("Expected error adding duplicate member")
	}
}

func TestHashRingEmptyMember(t *testing.T) {
	hr := NewHashRing()
	if err := hr.Add(""); err == nil {
		t.Fatal("Expected error adding empty member")
	}
}

func TestHashRingInvalidWeight(t *testing.T) {
	hr := NewHashRing()
	if err := hr.AddWeighted("server1", 0); err == nil {
		t.Fatal("Expected error with zero weight")
	}
	if err := hr.AddWeighted("server1", -1); err == nil {
		t.Fatal("Expected error with negative weight")
	}
}

func TestHashRingMembers(t *testing.T) {
	hr := NewHashRing()
	hr.Add("server1")
	hr.AddWeighted("server2", 3)

	members := hr.Members()
	if len(members) != 2 {
		t.Fatalf("Expected 2 members, got %d", len(members))
	}
	if members["server1"] != 1 {
		t.Fatalf("Expected weight 1 for server1, got %d", members["server1"])
	}
	if members["server2"] != 3 {
		t.Fatalf("Expected weight 3 for server2, got %d", members["server2"])
	}
}

func BenchmarkHashRingGet(b *testing.B) {
	hr := NewHashRing()
	for i := 0; i < 10; i++ {
		hr.Add(fmt.Sprintf("server%d", i))
	}

	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hr.Get(keys[i%len(keys)])
	}
}

func BenchmarkHashRingAdd(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hr := NewHashRing()
		for j := 0; j < 10; j++ {
			hr.Add(fmt.Sprintf("server%d", j))
		}
	}
}
