// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := MockDispatch{}
	_, c := prepare(t)
	s := NewTestStorage(t)

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

	procedure, err := NewTransferLeaderProcedure(dispatch, c, s, shardID, oldLeaderNodeName, newLeaderNodeName, uint64(1))
	re.NoError(err)
	err = procedure.Start(ctx)
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
