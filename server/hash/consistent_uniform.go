// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.
// This file fork from: https://github.com/buraksezer/consistent/blob/4516339c49db00f725fa89d0e3e7e970e4039af0/consistent.go
// Copyright (c) 2018 Burak Sezer
// All rights reserved.
//
// This code is licensed under the MIT License.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and / or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Package hash provides a consistent hashing function with bounded loads.
// For more information about the underlying algorithm, please take a look at
// https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
//
// We optimized and simplify this hash algorithm [implementation](https://github.com/buraksezer/consistent/issues/13)
package hash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/CeresDB/ceresmeta/pkg/assert"
)

var (
	// ErrInsufficientMemberCount represents an error which means there are not enough members to complete the task.
	ErrInsufficientMemberCount = errors.New("insufficient member count")

	// ErrMemberNotFound represents an error which means requested member could not be found in consistent hash ring.
	ErrMemberNotFound = errors.New("member could not be found in ring")

	// ErrHasherNotProvided will be thrown if the hasher is not provided.
	ErrHasherNotProvided = errors.New("hasher is required")

	// ErrInvalidReplication will be thrown if the replication factor is zero or negative.
	ErrInvalidReplicationFactor = errors.New("positive replication factor is required")

	// ErrInvalidNumPartitions will be thrown if the number of partitions is negative.
	ErrInvalidNumPartitions = errors.New("invalid number of the partitions")

	// ErrEmptyMembers will be thrown if no member is provided.
	ErrEmptyMembers = errors.New("at least one member is required")
)

type Hasher interface {
	Sum64([]byte) uint64
}

// Member interface represents a member in consistent hash ring.
type Member interface {
	String() string
}

// Config represents a structure to control consistent package.
type Config struct {
	// Hasher is responsible for generating unsigned, 64 bit hash of provided byte slice.
	Hasher Hasher

	// Keys are distributed among partitions. Prime numbers are good to
	// distribute keys uniformly. Select a big PartitionCount if you have
	// too many keys.
	ReplicationFactor int
}

type virtualNode uint64

// ConsistentUniformHash generates a uniform distribution of partitions over the members, and this distribution will keep as
// consistent as possible while the members has some tiny changes.
type ConsistentUniformHash struct {
	config        Config
	minLoad       float64
	maxLoad       float64
	numPartitions uint32
	// Member name => Member
	members map[string]Member
	// Member name => Member's load
	memLoads map[string]float64
	// partition id => index of the virtualNode in the sortedRing
	partitionDist map[int]int
	// The nodeToMems contains all the virtual nodes
	nodeToMems map[virtualNode]Member
	sortedRing []virtualNode
}

func (c *Config) Sanitize() error {
	if c.Hasher == nil {
		return ErrHasherNotProvided
	}

	if c.ReplicationFactor <= 0 {
		return ErrInvalidReplicationFactor
	}

	return nil
}

// BuildConsistentUniformHash creates and returns a new hash which is ensured to be uniform and as consistent as possible.
func BuildConsistentUniformHash(numPartitions int, members []Member, config Config) (*ConsistentUniformHash, error) {
	if err := config.Sanitize(); err != nil {
		return nil, err
	}
	if numPartitions < 0 {
		return nil, ErrInvalidNumPartitions
	}
	if len(members) == 0 {
		return nil, ErrEmptyMembers
	}

	numReplicatedNodes := len(members) * config.ReplicationFactor
	avgLoad := float64(numPartitions) / float64(len(members))
	c := &ConsistentUniformHash{
		config:        config,
		minLoad:       math.Floor(avgLoad),
		maxLoad:       math.Ceil(avgLoad),
		numPartitions: uint32(numPartitions),
		sortedRing:    make([]virtualNode, 0, numReplicatedNodes),
		members:       make(map[string]Member, len(members)),
		memLoads:      make(map[string]float64, len(members)),
		partitionDist: make(map[int]int, numPartitions),
		nodeToMems:    make(map[virtualNode]Member, numReplicatedNodes),
	}

	c.initializeVirtualNodes(members)
	c.distributePartitions()
	return c, nil
}

func (c *ConsistentUniformHash) distributePartitionWithLoad(partID, virtualNodeIdx int, allowedLoad float64) bool {
	// A fast path to avoid unnecessary loop.
	if allowedLoad == 0 {
		return false
	}

	var count int
	for {
		count++
		if count > len(c.sortedRing) {
			return false
		}
		i := c.sortedRing[virtualNodeIdx]
		member := c.nodeToMems[i]
		load := c.memLoads[member.String()]
		if load+1 <= allowedLoad {
			c.partitionDist[partID] = virtualNodeIdx
			c.memLoads[member.String()]++
			return true
		}
		virtualNodeIdx++
		if virtualNodeIdx >= len(c.sortedRing) {
			virtualNodeIdx = 0
		}
	}
}

func (c *ConsistentUniformHash) distributePartition(partID, virtualNodeIdx int) {
	ok := c.distributePartitionWithLoad(partID, virtualNodeIdx, c.MinLoad())
	if ok {
		return
	}

	ok = c.distributePartitionWithLoad(partID, virtualNodeIdx, c.MaxLoad())
	assert.Assertf(ok, "not enough room to distribute partitions")
}

func (c *ConsistentUniformHash) distributePartitions() {
	bs := make([]byte, 8)
	for partID := uint32(0); partID < c.numPartitions; partID++ {
		binary.LittleEndian.PutUint32(bs, partID)
		key := c.config.Hasher.Sum64(bs)
		idx := sort.Search(len(c.sortedRing), func(i int) bool {
			return c.sortedRing[i] >= virtualNode(key)
		})
		if idx >= len(c.sortedRing) {
			idx = 0
		}
		c.distributePartition(int(partID), idx)
	}
}

func (c *ConsistentUniformHash) MinLoad() float64 {
	return c.minLoad
}

func (c *ConsistentUniformHash) MaxLoad() float64 {
	return c.maxLoad
}

func (c *ConsistentUniformHash) initializeVirtualNodes(members []Member) {
	for _, mem := range members {
		for i := 0; i < c.config.ReplicationFactor; i++ {
			// TODO: Shall use a more generic hasher which receives multiple slices or string?
			key := []byte(fmt.Sprintf("%s%d", mem.String(), i))
			h := virtualNode(c.config.Hasher.Sum64(key))
			c.nodeToMems[h] = mem
			c.sortedRing = append(c.sortedRing, h)
		}
		c.members[mem.String()] = mem
	}

	sort.Slice(c.sortedRing, func(i int, j int) bool {
		return c.sortedRing[i] < c.sortedRing[j]
	})
}

// LoadDistribution exposes load distribution of members.
func (c *ConsistentUniformHash) LoadDistribution() map[string]float64 {
	// Create a thread-safe copy
	res := make(map[string]float64)
	for member, load := range c.memLoads {
		res[member] = load
	}
	return res
}

// GetPartitionOwner returns the owner of the given partition.
func (c *ConsistentUniformHash) GetPartitionOwner(partID int) Member {
	virtualNodeIdx, ok := c.partitionDist[partID]
	if !ok {
		return nil
	}
	virtualNode := c.sortedRing[virtualNodeIdx]
	mem, ok := c.nodeToMems[virtualNode]
	assert.Assertf(ok, "member must exist for the virtual node")
	return mem
}
