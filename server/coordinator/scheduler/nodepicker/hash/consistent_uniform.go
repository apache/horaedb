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

// This file is copied from:
// https://github.com/buraksezer/consistent/blob/4516339c49db00f725fa89d0e3e7e970e4039af0/consistent.go
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
	"slices"
	"sort"

	"github.com/CeresDB/horaemeta/pkg/assert"
	"github.com/CeresDB/horaemeta/pkg/log"
	"go.uber.org/zap"
)

// TODO: Modify these error definitions to coderr.
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

// hashSeparator is used to building the virtual node name for member.
// With this special separator, it will be hard to generate duplicate virtual node names.
const hashSeparator = "@$"

type Hasher interface {
	Sum64([]byte) uint64
}

// Member interface represents a member in consistent hash ring.
type Member interface {
	String() string
}

type PartitionAffinity struct {
	PartitionID               int
	NumAllowedOtherPartitions uint
}

// Config represents a structure to control consistent package.
type Config struct {
	// Hasher is responsible for generating unsigned, 64 bit hash of provided byte slice.
	Hasher Hasher

	// Keys are distributed among partitions. Prime numbers are good to
	// distribute keys uniformly. Select a big PartitionCount if you have
	// too many keys.
	ReplicationFactor int

	// The rule describes the partition affinity.
	PartitionAffinities []PartitionAffinity
}

type virtualNode uint64

// ConsistentUniformHash generates a uniform distribution of partitions over the members, and this distribution will keep as
// consistent as possible while the members has some tiny changes.
type ConsistentUniformHash struct {
	config        Config
	minLoad       int
	maxLoad       int
	numPartitions uint32
	// Member name => Member
	members map[string]Member
	// Member name => Partitions allocated to this member
	memPartitions map[string]map[int]struct{}
	// Partition ID => index of the virtualNode in the sortedRing
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
	minLoad := int(math.Floor(avgLoad))
	maxLoad := int(math.Ceil(avgLoad))

	memPartitions := make(map[string]map[int]struct{}, len(members))
	for _, mem := range members {
		memPartitions[mem.String()] = make(map[int]struct{}, maxLoad)
	}

	// Sort the affinity rule to ensure consistency.
	sort.Slice(config.PartitionAffinities, func(i, j int) bool {
		return config.PartitionAffinities[i].PartitionID < config.PartitionAffinities[j].PartitionID
	})
	c := &ConsistentUniformHash{
		config:        config,
		minLoad:       minLoad,
		maxLoad:       maxLoad,
		numPartitions: uint32(numPartitions),
		sortedRing:    make([]virtualNode, 0, numReplicatedNodes),
		memPartitions: memPartitions,
		members:       make(map[string]Member, len(members)),
		partitionDist: make(map[int]int, numPartitions),
		nodeToMems:    make(map[virtualNode]Member, numReplicatedNodes),
	}

	c.initializeVirtualNodes(members)
	c.distributePartitions()
	c.ensureAffinity()
	return c, nil
}

func (c *ConsistentUniformHash) distributePartitionWithLoad(partID, virtualNodeIdx int, allowedLoad int) bool {
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
		partitions, ok := c.memPartitions[member.String()]
		assert.Assert(ok)

		if len(partitions)+1 <= allowedLoad {
			c.partitionDist[partID] = virtualNodeIdx
			partitions[partID] = struct{}{}
			return true
		}
		virtualNodeIdx++
		if virtualNodeIdx >= len(c.sortedRing) {
			virtualNodeIdx = 0
		}
	}
}

func (c *ConsistentUniformHash) distributePartition(partID, virtualNodeIdx int) {
	ok := c.distributePartitionWithLoad(partID, virtualNodeIdx, c.minLoad)
	if ok {
		return
	}

	ok = c.distributePartitionWithLoad(partID, virtualNodeIdx, c.maxLoad)
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

func (c *ConsistentUniformHash) MinLoad() uint {
	return uint(c.minLoad)
}

func (c *ConsistentUniformHash) MaxLoad() uint {
	return uint(c.maxLoad)
}

// LoadDistribution exposes load distribution of members.
func (c *ConsistentUniformHash) LoadDistribution() map[string]uint {
	loads := make(map[string]uint, len(c.memPartitions))
	for member, partitions := range c.memPartitions {
		loads[member] = uint(len(partitions))
	}
	return loads
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

func (c *ConsistentUniformHash) initializeVirtualNodes(members []Member) {
	// Ensure the order of members to avoid inconsistency caused by hash collisions.
	sort.Slice(members, func(i, j int) bool {
		return members[i].String() < members[j].String()
	})

	for _, mem := range members {
		for i := 0; i < c.config.ReplicationFactor; i++ {
			// TODO: Shall use a more generic hasher which receives multiple slices or string?
			key := []byte(fmt.Sprintf("%s%s%d", mem.String(), hashSeparator, i))
			h := virtualNode(c.config.Hasher.Sum64(key))

			oldMem, ok := c.nodeToMems[h]
			if ok {
				log.Warn("found hash collision", zap.String("oldMem", oldMem.String()), zap.String("newMem", mem.String()))
			}

			c.nodeToMems[h] = mem
			c.sortedRing = append(c.sortedRing, h)
		}
		c.members[mem.String()] = mem
	}

	sort.Slice(c.sortedRing, func(i int, j int) bool {
		return c.sortedRing[i] < c.sortedRing[j]
	})
}

func (c *ConsistentUniformHash) ensureAffinity() {
	offloadedMems := make(map[string]struct{}, len(c.config.PartitionAffinities))

	for _, affinity := range c.config.PartitionAffinities {
		partID := affinity.PartitionID
		vNodeIdx := c.partitionDist[partID]
		vNode := c.sortedRing[vNodeIdx]
		mem, ok := c.nodeToMems[vNode]
		assert.Assert(ok)
		offloadedMems[mem.String()] = struct{}{}

		allowedLoad := int(affinity.NumAllowedOtherPartitions) + 1
		memPartIDs, ok := c.memPartitions[mem.String()]
		assert.Assert(ok)
		memLoad := len(memPartIDs)
		if memLoad > allowedLoad {
			c.offloadMember(mem, memPartIDs, partID, allowedLoad, offloadedMems)
		}
	}
}

// offloadMember tries to offload the given member by moving its partitions to other members.
func (c *ConsistentUniformHash) offloadMember(mem Member, memPartitions map[int]struct{}, retainedPartID, numAllowedParts int, offloadedMems map[string]struct{}) {
	assert.Assertf(numAllowedParts >= 1, "At least the partition itself should be allowed")
	partIDsToOffload := make([]int, 0, len(memPartitions)-numAllowedParts)
	// The `retainedPartID` must be retained.
	numRetainedParts := 1
	for partID := range memPartitions {
		if partID == retainedPartID {
			continue
		}

		if numRetainedParts < numAllowedParts {
			numRetainedParts++
			continue
		}

		partIDsToOffload = append(partIDsToOffload, partID)
	}

	slices.Sort(partIDsToOffload)
	for _, partID := range partIDsToOffload {
		c.offloadPartition(partID, mem, offloadedMems)
	}
}

func (c *ConsistentUniformHash) offloadPartition(sourcePartID int, sourceMem Member, blackedMembers map[string]struct{}) {
	// Ensure all members' load smaller than the max load as much as possible.
	loadUpperBound := c.numPartitions
	for load := c.maxLoad; load < int(loadUpperBound); load++ {
		if done := c.offloadPartitionWithAllowedLoad(sourcePartID, sourceMem, load, blackedMembers); done {
			return
		}
	}

	log.Warn("failed to offload partition")
}

func (c *ConsistentUniformHash) offloadPartitionWithAllowedLoad(sourcePartID int, sourceMem Member, allowedMaxLoad int, blackedMembers map[string]struct{}) bool {
	vNodeIdx := c.partitionDist[sourcePartID]
	// Skip the first member which must not be the target to move.
	for loopCnt := 1; loopCnt < len(c.sortedRing); loopCnt++ {
		vNodeIdx++
		if vNodeIdx == len(c.sortedRing) {
			vNodeIdx = 0
		}

		vNode := c.sortedRing[vNodeIdx]
		mem, ok := c.nodeToMems[vNode]
		assert.Assert(ok)

		// Check whether this member is blacked.
		if _, blacked := blackedMembers[mem.String()]; blacked {
			continue
		}

		memPartitions, ok := c.memPartitions[mem.String()]
		assert.Assert(ok)
		memLoad := len(memPartitions)
		// Check whether the member's load is too allowed.
		if memLoad+1 > allowedMaxLoad {
			continue
		}

		// The member meets the requirement, let's move the `sourcePartID` to this member.
		memPartitions[sourcePartID] = struct{}{}
		c.partitionDist[sourcePartID] = vNodeIdx
		sourceMemPartitions, ok := c.memPartitions[sourceMem.String()]
		assert.Assert(ok)
		delete(sourceMemPartitions, sourcePartID)
		return true
	}

	return false
}
