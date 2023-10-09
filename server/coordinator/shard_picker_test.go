// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/storage"
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
		SchemaName:    test.TestSchemaName,
		TableName:     "test",
		PartitionInfo: storage.PartitionInfo{},
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
}
