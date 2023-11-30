/*
 * Copyright 2022 The HoraeDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nodepicker_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/CeresDB/horaemeta/server/cluster/metadata"
	"github.com/CeresDB/horaemeta/server/coordinator/scheduler/nodepicker"
	"github.com/CeresDB/horaemeta/server/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	nodeLength            = 3
	selectOnlineNodeIndex = 1
	defaultTotalShardNum  = 10
)

func TestNodePicker(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	nodePicker := nodepicker.NewConsistentUniformHashNodePicker(zap.NewNop())

	var nodes []metadata.RegisteredNode
	config := nodepicker.Config{
		NumTotalShards:    defaultTotalShardNum,
		ShardAffinityRule: nil,
	}
	_, err := nodePicker.PickNode(ctx, config, []storage.ShardID{0}, nodes)
	re.Error(err)

	for i := 0; i < nodeLength; i++ {
		node := storage.Node{
			Name:          strconv.Itoa(i),
			NodeStats:     storage.NewEmptyNodeStats(),
			LastTouchTime: generateLastTouchTime(time.Minute),
			State:         storage.NodeStateUnknown,
		}
		nodes = append(nodes, metadata.RegisteredNode{
			Node:       node,
			ShardInfos: nil,
		})
	}
	_, err = nodePicker.PickNode(ctx, config, []storage.ShardID{0}, nodes)
	re.Error(err)

	nodes = nodes[:0]
	for i := 0; i < nodeLength; i++ {
		node := storage.Node{
			Name:          strconv.Itoa(i),
			NodeStats:     storage.NewEmptyNodeStats(),
			LastTouchTime: generateLastTouchTime(0),
			State:         storage.NodeStateUnknown,
		}
		nodes = append(nodes, metadata.RegisteredNode{
			Node:       node,
			ShardInfos: nil,
		})
	}
	_, err = nodePicker.PickNode(ctx, config, []storage.ShardID{0}, nodes)
	re.NoError(err)

	nodes = nodes[:0]
	for i := 0; i < nodeLength; i++ {
		node := storage.Node{
			Name:          strconv.Itoa(i),
			NodeStats:     storage.NewEmptyNodeStats(),
			LastTouchTime: generateLastTouchTime(time.Minute),
			State:         storage.NodeStateUnknown,
		}
		nodes = append(nodes, metadata.RegisteredNode{
			Node:       node,
			ShardInfos: nil,
		})
	}
	nodes[selectOnlineNodeIndex].Node.LastTouchTime = uint64(time.Now().UnixMilli())
	shardNodeMapping, err := nodePicker.PickNode(ctx, config, []storage.ShardID{0}, nodes)
	re.NoError(err)
	re.Equal(strconv.Itoa(selectOnlineNodeIndex), shardNodeMapping[0].Node.Name)
}

func TestUniformity(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	nodePicker := nodepicker.NewConsistentUniformHashNodePicker(zap.NewNop())
	mapping := allocShards(ctx, nodePicker, 30, 256, re)
	maxShardNum := 256/30 + 1
	for _, shards := range mapping {
		re.LessOrEqual(len(shards), maxShardNum)
	}

	// Verify that the result of hash remains unchanged through the same nodes and shards.
	t.Log("Verify that the result of hash remains unchanged through the same nodes and shards")
	newMapping := allocShards(ctx, nodePicker, 30, 256, re)
	maxShardNum = 256/30 + 1
	for _, shards := range newMapping {
		re.LessOrEqual(len(shards), maxShardNum)
	}

	for nodeName, shardIds := range mapping {
		newShardIDs := newMapping[nodeName]
		diffShardID := diffShardIds(shardIds, newShardIDs)
		println(fmt.Sprintf("diff shardID, nodeName:%s, diffShardIDs:%d", nodeName, len(diffShardID)))
		re.Equal(0, len(diffShardID))
	}

	// Add new node and testing shard rebalanced.
	t.Log("Add new node and testing shard rebalanced")
	newMapping = allocShards(ctx, nodePicker, 31, 256, re)
	maxShardNum = 256/31 + 1
	for _, shards := range newMapping {
		re.LessOrEqual(len(shards), maxShardNum)
	}
	maxDiffNum := 5
	for nodeName, shardIds := range mapping {
		newShardIDs := newMapping[nodeName]
		diffShardID := diffShardIds(shardIds, newShardIDs)
		re.LessOrEqual(len(diffShardID), maxDiffNum)
	}

	// Add new shard and testing shard rebalanced.
	t.Log("Add new shard and testing shard rebalanced")
	newShardMapping := allocShards(ctx, nodePicker, 30, 257, re)
	maxShardNum = 257/31 + 1
	for _, shards := range newShardMapping {
		re.LessOrEqual(len(shards), maxShardNum)
	}
	maxDiffNum = 5
	for nodeName, shardIds := range newShardMapping {
		newShardIDs := newMapping[nodeName]
		diffShardID := diffShardIds(shardIds, newShardIDs)
		re.LessOrEqual(len(diffShardID), maxDiffNum)
	}
}

func allocShards(ctx context.Context, nodePicker nodepicker.NodePicker, nodeNum int, shardNum int, re *require.Assertions) map[string][]int {
	var nodes []metadata.RegisteredNode
	for i := 0; i < nodeNum; i++ {
		node := storage.Node{
			Name:          strconv.Itoa(i),
			NodeStats:     storage.NewEmptyNodeStats(),
			LastTouchTime: generateLastTouchTime(0),
			State:         storage.NodeStateUnknown,
		}
		nodes = append(nodes, metadata.RegisteredNode{
			Node:       node,
			ShardInfos: nil,
		})
	}
	mapping := make(map[string][]int, 0)
	shardIDs := make([]storage.ShardID, 0, shardNum)
	for i := 0; i < shardNum; i++ {
		shardIDs = append(shardIDs, storage.ShardID(i))
	}
	config := nodepicker.Config{
		NumTotalShards:    uint32(shardNum),
		ShardAffinityRule: nil,
	}
	shardNodeMapping, err := nodePicker.PickNode(ctx, config, shardIDs, nodes)
	re.NoError(err)
	for shardID, node := range shardNodeMapping {
		mapping[node.Node.Name] = append(mapping[node.Node.Name], int(shardID))
	}

	return mapping
}

func generateLastTouchTime(duration time.Duration) uint64 {
	return uint64(time.Now().UnixMilli() - int64(duration))
}

func diffShardIds(oldShardIDs, newShardIDs []int) []int {
	diff := make(map[int]bool, 0)
	for i := 0; i < len(oldShardIDs); i++ {
		diff[oldShardIDs[i]] = false
	}
	for i := 0; i < len(newShardIDs); i++ {
		if diff[newShardIDs[i]] == false {
			diff[newShardIDs[i]] = true
		}
	}

	var result []int
	for k, v := range diff {
		if !v {
			result = append(result, k)
		}
	}
	return result
}
