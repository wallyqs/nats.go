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
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// DefaultVNodes is the default number of virtual nodes per member.
// This follows the nginx upstream consistent hashing convention
// where each member gets weight * 160 points on the ring.
const DefaultVNodes = 160

// HashRing implements an nginx-style consistent hash ring using
// CRC32 hashing with virtual nodes (ketama-like). This provides
// minimal key remapping when members are added or removed.
type HashRing struct {
	mu      sync.RWMutex
	points  []point
	members map[string]int // member name -> weight
}

type point struct {
	hash   uint32
	member string
}

// NewHashRing creates a new consistent hash ring.
func NewHashRing() *HashRing {
	return &HashRing{
		members: make(map[string]int),
	}
}

// Add adds a member to the ring with a default weight of 1.
func (h *HashRing) Add(member string) error {
	return h.AddWeighted(member, 1)
}

// AddWeighted adds a member to the ring with the given weight.
// The weight controls how many virtual nodes are placed on the ring
// (weight * DefaultVNodes), giving higher-weighted members proportionally
// more traffic.
func (h *HashRing) AddWeighted(member string, weight int) error {
	if member == "" {
		return errors.New("nats: hash ring member must not be empty")
	}
	if weight < 1 {
		return errors.New("nats: hash ring weight must be at least 1")
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.members[member]; exists {
		return errors.New("nats: hash ring member already exists")
	}

	h.members[member] = weight
	h.addPoints(member, weight)
	return nil
}

// Remove removes a member from the ring.
func (h *HashRing) Remove(member string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.members[member]; !exists {
		return errors.New("nats: hash ring member not found")
	}

	delete(h.members, member)
	h.rebuild()
	return nil
}

// Get returns the member responsible for the given key.
// Uses the nginx consistent hashing approach: CRC32 hash of the key,
// then binary search on the sorted ring to find the next point.
func (h *HashRing) Get(key string) (string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.points) == 0 {
		return "", errors.New("nats: hash ring is empty")
	}

	hash := crc32Hash(key)

	// Binary search for the first point >= hash.
	idx := sort.Search(len(h.points), func(i int) bool {
		return h.points[i].hash >= hash
	})

	// Wrap around to the first point if we're past the end.
	if idx == len(h.points) {
		idx = 0
	}

	return h.points[idx].member, nil
}

// GetN returns up to n distinct members closest to the given key
// on the ring. Useful for replication where a key should map to
// multiple members.
func (h *HashRing) GetN(key string, n int) ([]string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.points) == 0 {
		return nil, errors.New("nats: hash ring is empty")
	}

	totalMembers := len(h.members)
	if n > totalMembers {
		n = totalMembers
	}

	hash := crc32Hash(key)
	idx := sort.Search(len(h.points), func(i int) bool {
		return h.points[i].hash >= hash
	})
	if idx == len(h.points) {
		idx = 0
	}

	seen := make(map[string]struct{}, n)
	result := make([]string, 0, n)

	for len(result) < n {
		member := h.points[idx].member
		if _, ok := seen[member]; !ok {
			seen[member] = struct{}{}
			result = append(result, member)
		}
		idx = (idx + 1) % len(h.points)
	}

	return result, nil
}

// Members returns the current set of members and their weights.
func (h *HashRing) Members() map[string]int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make(map[string]int, len(h.members))
	for k, v := range h.members {
		result[k] = v
	}
	return result
}

// Size returns the number of members in the ring.
func (h *HashRing) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.members)
}

// addPoints generates virtual node points for a member. Must be called
// with the lock held. This follows the nginx approach of generating
// weight*160 points using CRC32 over packed bytes.
func (h *HashRing) addPoints(member string, weight int) {
	numPoints := weight * DefaultVNodes
	for i := 0; i < numPoints; i++ {
		hash := crc32Hash(member + "#" + strconv.Itoa(i))
		h.points = append(h.points, point{hash: hash, member: member})
	}
	sort.Slice(h.points, func(i, j int) bool {
		return h.points[i].hash < h.points[j].hash
	})
}

// rebuild recreates the ring from the current members. Must be called
// with the lock held.
func (h *HashRing) rebuild() {
	h.points = h.points[:0]
	for member, weight := range h.members {
		numPoints := weight * DefaultVNodes
		for i := 0; i < numPoints; i++ {
			hash := crc32Hash(member + "#" + strconv.Itoa(i))
			h.points = append(h.points, point{hash: hash, member: member})
		}
	}
	sort.Slice(h.points, func(i, j int) bool {
		return h.points[i].hash < h.points[j].hash
	})
}

// crc32Hash computes a CRC32 hash of the given key, matching the
// approach used by nginx for consistent hashing.
func crc32Hash(key string) uint32 {
	b := []byte(key)
	// Use CRC32 IEEE (same polynomial as nginx).
	h := crc32.ChecksumIEEE(b)
	// Convert to big-endian representation for consistent ordering,
	// matching nginx behavior.
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], h)
	return binary.BigEndian.Uint32(buf[:])
}
