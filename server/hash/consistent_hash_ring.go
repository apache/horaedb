// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.
// The consistent hash ring refers to [groupcache](https://github.com/golang/groupcache/blob/4a4ac3fbac33b83bb138f808c8945a2812023fc4/consistenthash/consistenthash.go)
/*
Copyright 2013 Google Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
     http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type ConsistentHashRing struct {
	hash     Hash
	replicas int
	ring     []int
	nodes    map[int]string
}

func New(replicas int, fn Hash) *ConsistentHashRing {
	m := &ConsistentHashRing{
		replicas: replicas,
		hash:     fn,
		nodes:    make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns true if there are no items available.
func (h *ConsistentHashRing) IsEmpty() bool {
	return len(h.ring) == 0
}

// Add adds some keys to the hash.
func (h *ConsistentHashRing) Add(nodes ...string) {
	for _, node := range nodes {
		for i := 0; i < h.replicas; i++ {
			hash := int(h.hash([]byte(strconv.Itoa(i) + node)))
			h.ring = append(h.ring, hash)
			h.nodes[hash] = node
		}
	}
	sort.Ints(h.ring)
}

// Get gets the closest item in the hash to the provided key.
func (h *ConsistentHashRing) Get(key string) string {
	if h.IsEmpty() {
		return ""
	}

	hash := int(h.hash([]byte(key)))

	// Binary search for appropriate replica.
	idx := sort.Search(len(h.ring), func(i int) bool { return h.ring[i] >= hash })

	// Means we have cycled back to the first replica.
	if idx == len(h.ring) {
		idx = 0
	}

	return h.nodes[h.ring[idx]]
}
