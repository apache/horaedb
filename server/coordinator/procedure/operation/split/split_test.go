// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package split

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/operation/scatter"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestSplit(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := test.MockDispatch{}
	_, c := scatter.Prepare(t)
	s := test.NewTestStorage(t)

	getNodeShardsResult, err := c.GetNodeShards(ctx)
	re.NoError(err)

	// Randomly select a shardNode to split.
	createTableNodeShard := getNodeShardsResult.NodeShards[0].ShardNode

	// Create some tables in this shard.
	createTableResult, err := c.CreateTable(ctx, cluster.CreateTableRequest{
		ShardID:    createTableNodeShard.ID,
		SchemaName: test.TestSchemaName,
		TableName:  test.TestTableName0,
	})
	re.NoError(err)
	_, err = c.CreateTable(ctx, cluster.CreateTableRequest{
		ShardID:    createTableNodeShard.ID,
		SchemaName: test.TestSchemaName,
		TableName:  test.TestTableName1,
	})
	re.NoError(err)

	// Split one table from this shard.
	getNodeShardResult, err := c.GetShardNodeByTableIDs([]storage.TableID{createTableResult.Table.ID})
	targetShardNode := getNodeShardResult.ShardNodes[createTableResult.Table.ID][0]
	re.NoError(err)
	newShardID, err := c.AllocShardID(ctx)
	re.NoError(err)
	p := NewProcedure(1, dispatch, s, c, test.TestSchemaName, targetShardNode.ID, storage.ShardID(newShardID), []string{test.TestTableName0}, targetShardNode.NodeName)
	err = p.Start(ctx)
	re.NoError(err)

	// Validate split result:
	// 1. Shards on node, split shard and new shard must be all exists on node.
	// 2. Tables mapping of split shard and new shard must be all exists.
	// 3. Tables in table mapping must be correct, the split table only exists on the new shard.
	getNodeShardsResult, err = c.GetNodeShards(ctx)
	re.NoError(err)

	nodeShardsMapping := make(map[storage.ShardID]cluster.ShardNodeWithVersion, 0)
	for _, nodeShard := range getNodeShardsResult.NodeShards {
		nodeShardsMapping[nodeShard.ShardNode.ID] = nodeShard
	}
	splitNodeShard := nodeShardsMapping[targetShardNode.ID]
	newNodeShard := nodeShardsMapping[storage.ShardID(newShardID)]
	re.NotNil(splitNodeShard)
	re.NotNil(newNodeShard)

	shardTables := c.GetShardTables([]storage.ShardID{targetShardNode.ID, storage.ShardID(newShardID)})
	splitShardTables := shardTables[targetShardNode.ID]
	newShardTables := shardTables[storage.ShardID(newShardID)]
	re.NotNil(splitShardTables)
	re.NotNil(newShardTables)

	splitShardTablesMapping := make(map[string]cluster.TableInfo, 0)
	for _, table := range splitShardTables.Tables {
		splitShardTablesMapping[table.Name] = table
	}
	_, exists := splitShardTablesMapping[test.TestTableName0]
	re.False(exists)

	newShardTablesMapping := make(map[string]cluster.TableInfo, 0)
	for _, table := range newShardTables.Tables {
		newShardTablesMapping[table.Name] = table
	}
	_, exists = newShardTablesMapping[test.TestTableName0]
	re.True(exists)
}
