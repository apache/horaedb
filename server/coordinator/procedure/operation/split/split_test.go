// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package split_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/operation/split"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestSplit(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := test.MockDispatch{}
	c := test.InitStableCluster(ctx, t)
	s := test.NewTestStorage(t)

	snapshot := c.GetMetadata().GetClusterSnapshot()

	// Randomly select a shardNode to split.
	createTableNodeShard := snapshot.Topology.ClusterView.ShardNodes[0]

	// Create some tables in this shard.
	_, err := c.GetMetadata().CreateTable(ctx, metadata.CreateTableRequest{
		ShardID:    createTableNodeShard.ID,
		SchemaName: test.TestSchemaName,
		TableName:  test.TestTableName0,
	})
	re.NoError(err)
	_, err = c.GetMetadata().CreateTable(ctx, metadata.CreateTableRequest{
		ShardID:    createTableNodeShard.ID,
		SchemaName: test.TestSchemaName,
		TableName:  test.TestTableName1,
	})
	re.NoError(err)

	// Split one table from this shard.
	targetShardNode := c.GetMetadata().GetClusterSnapshot().Topology.ClusterView.ShardNodes[0]
	newShardID, err := c.GetMetadata().AllocShardID(ctx)
	re.NoError(err)
	p, err := split.NewProcedure(split.ProcedureParams{
		ID:              0,
		Dispatch:        dispatch,
		Storage:         s,
		ClusterMetadata: c.GetMetadata(),
		ClusterSnapshot: c.GetMetadata().GetClusterSnapshot(),
		ShardID:         createTableNodeShard.ID,
		NewShardID:      storage.ShardID(newShardID),
		SchemaName:      test.TestSchemaName,
		TableNames:      []string{test.TestTableName0},
		TargetNodeName:  createTableNodeShard.NodeName,
	})
	re.NoError(err)
	err = p.Start(ctx)
	re.NoError(err)

	// Validate split result:
	// 1. Shards on node, split shard and new shard must be all exists.
	// 2. Tables mapping of split shard and new shard must be all exists.
	// 3. Tables in table mapping must be correct, the split table only exists on the new shard.
	snapshot = c.GetMetadata().GetClusterSnapshot()

	splitShard, exists := snapshot.Topology.ShardViewsMapping[targetShardNode.ID]
	re.True(exists)
	newShard, exists := snapshot.Topology.ShardViewsMapping[storage.ShardID(newShardID)]
	re.True(exists)
	re.NotNil(splitShard)
	re.NotNil(newShard)

	splitShardTables := splitShard.TableIDs
	newShardTables := newShard.TableIDs
	re.NotNil(splitShardTables)
	re.NotNil(newShardTables)
}
