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

package coordinator_test

import (
	"context"
	"sort"
	"testing"

	"github.com/CeresDB/horaemeta/server/cluster/metadata"
	"github.com/CeresDB/horaemeta/server/coordinator"
	"github.com/CeresDB/horaemeta/server/coordinator/procedure/test"
	"github.com/CeresDB/horaemeta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestLeastTableShardPicker(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	c := test.InitStableCluster(ctx, t)
	snapshot := c.GetMetadata().GetClusterSnapshot()

	shardPicker := coordinator.NewLeastTableShardPicker()

	shardNodes, err := shardPicker.PickShards(ctx, snapshot, 4)
	re.NoError(err)
	re.Equal(len(shardNodes), 4)
	// Each shardNode should be different shard.
	shardIDs := map[storage.ShardID]struct{}{}
	for _, shardNode := range shardNodes {
		shardIDs[shardNode.ID] = struct{}{}
	}
	re.Equal(len(shardIDs), 4)

	shardNodes, err = shardPicker.PickShards(ctx, snapshot, 7)
	re.NoError(err)
	re.Equal(len(shardNodes), 7)
	// Each shardNode should be different shard.
	shardIDs = map[storage.ShardID]struct{}{}
	for _, shardNode := range shardNodes {
		shardIDs[shardNode.ID] = struct{}{}
	}
	re.Equal(len(shardIDs), 4)

	// Create table on shard 0.
	_, err = c.GetMetadata().CreateTable(ctx, metadata.CreateTableRequest{
		ShardID:       0,
		LatestVersion: 0,
		SchemaName:    test.TestSchemaName,
		TableName:     "test",
		PartitionInfo: storage.PartitionInfo{
			Info: nil,
		},
	})
	re.NoError(err)

	// shard 0 should not exist in pick result.
	shardNodes, err = shardPicker.PickShards(ctx, snapshot, 3)
	re.NoError(err)
	re.Equal(len(shardNodes), 3)
	for _, shardNode := range shardNodes {
		re.NotEqual(shardNode.ID, 0)
	}

	// drop shard node 1, shard 1 should not be picked.
	for _, shardNode := range snapshot.Topology.ClusterView.ShardNodes {
		if shardNode.ID == 1 {
			err = c.GetMetadata().DropShardNode(ctx, []storage.ShardNode{shardNode})
			re.NoError(err)
		}
	}
	shardNodes, err = shardPicker.PickShards(ctx, snapshot, 8)
	re.NoError(err)
	for _, shardNode := range shardNodes {
		re.NotEqual(shardNode.ID, 1)
	}

	checkPartitionTable(ctx, shardPicker, t, 50, 256, 20, 2)
	checkPartitionTable(ctx, shardPicker, t, 50, 256, 30, 2)
	checkPartitionTable(ctx, shardPicker, t, 50, 256, 40, 2)
	checkPartitionTable(ctx, shardPicker, t, 50, 256, 50, 2)
}

func checkPartitionTable(ctx context.Context, shardPicker coordinator.ShardPicker, t *testing.T, nodeNumber int, shardNumber int, subTableNumber int, maxDifference int) {
	re := require.New(t)

	var shardNodes []storage.ShardNode

	c := test.InitStableClusterWithConfig(ctx, t, nodeNumber, shardNumber)
	shardNodes, err := shardPicker.PickShards(ctx, c.GetMetadata().GetClusterSnapshot(), subTableNumber)
	re.NoError(err)

	nodeTableCountMapping := make(map[string]int, 0)
	for _, shardNode := range shardNodes {
		nodeTableCountMapping[shardNode.NodeName]++
	}

	// Ensure the difference in the number of tables is no greater than maxDifference
	var nodeTableNumberSlice []int
	for _, tableNumber := range nodeTableCountMapping {
		nodeTableNumberSlice = append(nodeTableNumberSlice, tableNumber)
	}
	sort.Ints(nodeTableNumberSlice)
	minTableNumber := nodeTableNumberSlice[0]
	maxTableNumber := nodeTableNumberSlice[len(nodeTableNumberSlice)-1]
	re.LessOrEqual(maxTableNumber-minTableNumber, maxDifference)
}
