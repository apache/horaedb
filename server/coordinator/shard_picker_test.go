// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/operation/scatter"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/stretchr/testify/require"
)

func TestRandomShardPicker(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	manager, _ := scatter.Prepare(t)

	shardPicker := NewRandomBalancedShardPicker(manager)
	nodeShards, err := shardPicker.PickShards(ctx, test.ClusterName, 2, false)
	re.NoError(err)

	// Verify the number of shards and ensure that they are not on the same node.
	re.Equal(len(nodeShards), 2)
	re.NotEqual(nodeShards[0].ShardNode.NodeName, nodeShards[1].ShardNode.NodeName)

	// ExpectShardNum is bigger than node number and enableDuplicateNode is false, it should be throw error.
	_, err = shardPicker.PickShards(ctx, test.ClusterName, 3, false)
	re.Error(err)

	// ExpectShardNum is bigger than node number and enableDuplicateNode is true, it should return correct shards.
	nodeShards, err = shardPicker.PickShards(ctx, test.ClusterName, 3, true)
	re.NoError(err)
	re.Equal(len(nodeShards), 3)
	nodeShards, err = shardPicker.PickShards(ctx, test.ClusterName, 4, true)
	re.NoError(err)
	re.Equal(len(nodeShards), 4)
	// ExpectShardNum is bigger than shard number.
	_, err = shardPicker.PickShards(ctx, test.ClusterName, 5, true)
	re.NoError(err)
}
