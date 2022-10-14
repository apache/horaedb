// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/stretchr/testify/require"
)

func TestTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := MockDispatch{}
	cluster := newTestCluster(ctx, t)

	// Initialize shard topology
	shardVies := make([]*clusterpb.Shard, 0)
	shard0 := &clusterpb.Shard{
		ShardRole: clusterpb.ShardRole_LEADER,
		Node:      nodeName0,
		Id:        0,
	}
	shardVies = append(shardVies, shard0)
	shard1 := &clusterpb.Shard{
		ShardRole: clusterpb.ShardRole_LEADER,
		Node:      nodeName0,
		Id:        1,
	}
	shardVies = append(shardVies, shard1)
	err := cluster.UpdateClusterTopology(ctx, clusterpb.ClusterTopology_STABLE, shardVies)
	re.NoError(err)

	// Create transfer leader procedure, oldLeader is shard0, newLeader is shard0 move to another node
	oldLeader := &clusterpb.Shard{
		ShardRole: clusterpb.ShardRole_LEADER,
		Node:      nodeName0,
		Id:        0,
	}
	newLeader := &clusterpb.Shard{
		ShardRole: clusterpb.ShardRole_LEADER,
		Node:      nodeName1,
		Id:        2,
	}
	procedure := NewTransferLeaderProcedure(dispatch, cluster, oldLeader, newLeader, uint64(1))
	err = procedure.Start(ctx)
	re.NoError(err)
	shardViews, err := cluster.GetClusterShardView()
	re.NoError(err)
	re.Equal(len(shardViews), 2)
	for _, shardView := range shardViews {
		shardID := shardView.GetId()
		if shardID == 2 {
			node := shardView.GetNode()
			re.Equal(nodeName1, node)
		}
	}
}
