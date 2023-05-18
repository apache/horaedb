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

	shardNodes, err := shardPicker.PickShards(ctx, snapshot, 3)
	re.NoError(err)
	re.Equal(len(shardNodes), 3)
	shardNodes, err = shardPicker.PickShards(ctx, snapshot, 4)
	re.NoError(err)
	re.Equal(len(shardNodes), 4)
	// ExpectShardNum is bigger than shard number.
	shardNodes, err = shardPicker.PickShards(ctx, snapshot, 5)
	re.NoError(err)
	re.Equal(len(shardNodes), 5)
	// TODO: Ensure that the shardNodes is average distributed on nodes and shards.
	shardNodes, err = shardPicker.PickShards(ctx, snapshot, 9)
	re.NoError(err)
	re.Equal(len(shardNodes), 9)
}
