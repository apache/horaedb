// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.
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

package hash

import (
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testMember string

func (tm testMember) String() string {
	return string(tm)
}

type testHasher struct{}

func (hs testHasher) Sum64(data []byte) uint64 {
	h := fnv.New64()
	_, _ = h.Write(data)
	return h.Sum64()
}

func buildTestMembers(n int) []Member {
	members := []Member{}
	for i := 0; i < n; i++ {
		member := testMember(fmt.Sprintf("node-%d", i))
		members = append(members, member)
	}

	return members
}

func checkUniform(t *testing.T, numPartitions, numMembers int) {
	members := buildTestMembers(numMembers)
	cfg := Config{
		ReplicationFactor:   127,
		Hasher:              testHasher{},
		PartitionAffinities: []PartitionAffinity{},
	}
	c, err := BuildConsistentUniformHash(numPartitions, members, cfg)
	assert.NoError(t, err)

	minLoad := c.MinLoad()
	maxLoad := c.MaxLoad()
	loadDistribution := c.LoadDistribution()
	for _, mem := range members {
		load, ok := loadDistribution[mem.String()]
		if ok {
			assert.GreaterOrEqual(t, load, minLoad)
			assert.LessOrEqual(t, load, maxLoad)
		} else {
			assert.Equal(t, 0.0, minLoad)
		}
	}
}

func TestZeroReplicationFactor(t *testing.T) {
	cfg := Config{
		ReplicationFactor:   0,
		Hasher:              testHasher{},
		PartitionAffinities: []PartitionAffinity{},
	}
	_, err := BuildConsistentUniformHash(0, []Member{testMember("")}, cfg)
	assert.Error(t, err)
}

func TestEmptyHasher(t *testing.T) {
	cfg := Config{
		Hasher:              nil,
		ReplicationFactor:   127,
		PartitionAffinities: []PartitionAffinity{},
	}
	_, err := BuildConsistentUniformHash(0, []Member{testMember("")}, cfg)
	assert.Error(t, err)
}

func TestEmptyMembers(t *testing.T) {
	cfg := Config{
		Hasher:              testHasher{},
		ReplicationFactor:   127,
		PartitionAffinities: []PartitionAffinity{},
	}
	_, err := BuildConsistentUniformHash(0, []Member{}, cfg)
	assert.Error(t, err)
}

func TestNegativeNumPartitions(t *testing.T) {
	cfg := Config{
		Hasher:              testHasher{},
		ReplicationFactor:   127,
		PartitionAffinities: []PartitionAffinity{},
	}
	_, err := BuildConsistentUniformHash(-1, []Member{testMember("")}, cfg)
	assert.Error(t, err)
}

func TestUniform(t *testing.T) {
	checkUniform(t, 23, 8)
	checkUniform(t, 128, 72)
	checkUniform(t, 10, 72)
	checkUniform(t, 1, 8)
	checkUniform(t, 0, 8)
	checkUniform(t, 100, 1)
}

func computeDiffBetweenDist(t *testing.T, oldDist, newDist map[int]string) int {
	numDiffs := 0
	assert.Equal(t, len(oldDist), len(newDist))
	for partID, oldMem := range oldDist {
		newMem, ok := newDist[partID]
		assert.True(t, ok)
		if newMem != oldMem {
			numDiffs++
		}
	}

	return numDiffs
}

func checkConsistent(t *testing.T, numPartitions, numMembers, maxDiff int) {
	members := buildTestMembers(numMembers)
	cfg := Config{
		Hasher:              testHasher{},
		ReplicationFactor:   127,
		PartitionAffinities: []PartitionAffinity{},
	}
	c, err := BuildConsistentUniformHash(numPartitions, members, cfg)
	assert.NoError(t, err)

	distribution := make(map[int]string, numPartitions)
	for partID := 0; partID < numPartitions; partID++ {
		distribution[partID] = c.GetPartitionOwner(partID).String()
	}
	sortedRing := c.sortedRing
	nodeToMems := c.nodeToMems

	{
		newMembers := make([]Member, 0, numMembers)
		for i := numMembers - 1; i >= 0; i-- {
			newMembers = append(newMembers, members[i])
		}
		c, err := BuildConsistentUniformHash(numPartitions, newMembers, cfg)
		assert.NoError(t, err)

		newSortedRing := c.sortedRing
		assert.Equal(t, sortedRing, newSortedRing)

		newNodeToMems := c.nodeToMems
		assert.Equal(t, nodeToMems, newNodeToMems)

		newDistribution := make(map[int]string, numPartitions)
		for partID := 0; partID < numPartitions; partID++ {
			newDistribution[partID] = c.GetPartitionOwner(partID).String()
		}
		numDiffs := computeDiffBetweenDist(t, distribution, newDistribution)
		assert.Equal(t, numDiffs, 0)
	}

	oldMem0 := members[0].String()
	newMem0 := "new-node-0"
	members[0] = testMember(newMem0)
	c, err = BuildConsistentUniformHash(numPartitions, members, cfg)
	assert.NoError(t, err)

	numDiffs := 0
	for partID := 0; partID < numPartitions; partID++ {
		newMem := c.GetPartitionOwner(partID).String()
		oldMem := distribution[partID]
		if newMem0 == newMem && oldMem != oldMem0 {
			numDiffs++
			continue
		}

		if newMem != oldMem {
			numDiffs++
		}
	}

	assert.LessOrEqual(t, numDiffs, maxDiff)
}

func TestConsistency(t *testing.T) {
	checkConsistent(t, 120, 20, 12)
	checkConsistent(t, 100, 20, 11)
	checkConsistent(t, 128, 70, 10)
	checkConsistent(t, 256, 30, 42)
	checkConsistent(t, 17, 5, 10)
}

func checkAffinity(t *testing.T, numPartitions, numMembers int, affinities []PartitionAffinity, revisedMaxLoad uint) {
	members := buildTestMembers(numMembers)
	cfg := Config{
		ReplicationFactor:   127,
		Hasher:              testHasher{},
		PartitionAffinities: affinities,
	}
	c, err := BuildConsistentUniformHash(numPartitions, members, cfg)
	assert.NoError(t, err)

	minLoad := c.MinLoad()
	maxLoad := c.MaxLoad()
	if maxLoad < revisedMaxLoad {
		maxLoad = revisedMaxLoad
	}
	loadDistribution := c.LoadDistribution()
	for _, mem := range members {
		load, ok := loadDistribution[mem.String()]
		if !ok {
			assert.Equal(t, 0.0, minLoad)
		}
		assert.LessOrEqual(t, load, maxLoad)
	}

	for _, affinity := range affinities {
		mem := c.GetPartitionOwner(affinity.PartitionID)
		load := loadDistribution[mem.String()]
		allowedMaxLoad := affinity.NumAllowedOtherPartitions + 1
		assert.LessOrEqual(t, load, allowedMaxLoad)
	}

	distribution := make(map[int]string, numPartitions)
	for partID := 0; partID < numPartitions; partID++ {
		distribution[partID] = c.GetPartitionOwner(partID).String()
	}
	{
		newMembers := make([]Member, 0, numMembers)
		for i := numMembers - 1; i >= 0; i-- {
			newMembers = append(newMembers, members[i])
		}
		c, err := BuildConsistentUniformHash(numPartitions, newMembers, cfg)
		assert.NoError(t, err)

		newDistribution := make(map[int]string, numPartitions)
		for partID := 0; partID < numPartitions; partID++ {
			newDistribution[partID] = c.GetPartitionOwner(partID).String()
		}
		numDiffs := computeDiffBetweenDist(t, distribution, newDistribution)
		assert.Equal(t, numDiffs, 0)
	}
}

func TestAffinity(t *testing.T) {
	rule := []PartitionAffinity{}
	checkAffinity(t, 120, 72, rule, 0)
	checkAffinity(t, 0, 72, rule, 0)

	rule = []PartitionAffinity{
		{0, 0},
		{1, 0},
		{2, 120},
	}
	checkAffinity(t, 3, 72, rule, 0)
	checkAffinity(t, 72, 72, rule, 0)

	rule = []PartitionAffinity{
		{7, 0},
		{31, 0},
		{41, 0},
		{45, 0},
		{58, 0},
		{81, 0},
		{87, 0},
		{88, 0},
		{89, 0},
	}
	checkAffinity(t, 128, 72, rule, 0)
}

func TestInvalidAffinity(t *testing.T) {
	// This affinity rule requires at least 4 member, but it should work too.
	rule := []PartitionAffinity{
		{0, 0},
		{1, 0},
		{2, 0},
		{3, 0},
	}

	members := buildTestMembers(3)
	cfg := Config{
		ReplicationFactor:   127,
		Hasher:              testHasher{},
		PartitionAffinities: rule,
	}
	_, err := BuildConsistentUniformHash(4, members, cfg)
	assert.NoError(t, err)
}
