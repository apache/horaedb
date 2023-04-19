// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/stretchr/testify/require"
)

func TestRandomShardPicker(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	c := test.InitStableCluster(ctx, t)
	snapshot := c.GetMetadata().GetClusterSnapshot()

	shardPicker := coordinator.NewRandomBalancedShardPicker()
	shardNodes, err := shardPicker.PickShards(ctx, snapshot, 2, false)
	re.NoError(err)

	// Verify the number of shards and ensure that they are not on the same node.
	re.Equal(len(shardNodes), 2)
	re.NotEqual(shardNodes[0].NodeName, shardNodes[1].NodeName)

	// ExpectShardNum is bigger than node number and enableDuplicateNode is false, it should be throw error.
	_, err = shardPicker.PickShards(ctx, snapshot, 3, false)
	re.Error(err)

	// ExpectShardNum is bigger than node number and enableDuplicateNode is true, it should return correct shards.
	shardNodes, err = shardPicker.PickShards(ctx, snapshot, 3, true)
	re.NoError(err)
	re.Equal(len(shardNodes), 3)
	shardNodes, err = shardPicker.PickShards(ctx, snapshot, 4, true)
	re.NoError(err)
	re.Equal(len(shardNodes), 4)
	// ExpectShardNum is bigger than shard number.
	_, err = shardPicker.PickShards(ctx, snapshot, 5, true)
	re.NoError(err)
}
