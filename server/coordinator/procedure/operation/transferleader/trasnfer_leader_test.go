// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package transferleader

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/operation/scatter"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := test.MockDispatch{}
	_, c := scatter.Prepare(t)
	s := test.NewTestStorage(t)

	getNodeShardsResult, err := c.GetNodeShards(ctx)
	re.NoError(err)

	// Randomly select a node and shard to transfer leader.
	oldLeaderNodeName := getNodeShardsResult.NodeShards[0].ShardNode.NodeName
	shardID := getNodeShardsResult.NodeShards[0].ShardNode.ID

	// Randomly select another node as new leader
	registerNodes := c.GetRegisteredNodes()
	var newLeaderNodeName string
	found := false
	for _, node := range registerNodes {
		if node.Node.Name != oldLeaderNodeName {
			newLeaderNodeName = node.Node.Name
			found = true
		}
	}
	re.Equal(true, found)

	p, err := NewProcedure(dispatch, c, s, shardID, oldLeaderNodeName, newLeaderNodeName, uint64(1))
	re.NoError(err)
	err = p.Start(ctx)
	re.NoError(err)

	shardNodes, err := c.GetShardNodesByShardID(shardID)
	re.NoError(err)
	found = false
	var newLeaderShardNode storage.ShardNode
	for _, shardNode := range shardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			found = true
			newLeaderShardNode = shardNode
		}
	}
	re.Equal(true, found)
	re.Equal(newLeaderNodeName, newLeaderShardNode.NodeName)
}
