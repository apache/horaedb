// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package metadata_test

import (
	"context"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestClusterMetadata(t *testing.T) {
	ctx := context.Background()
	re := require.New(t)
	metadata := test.InitStableCluster(ctx, t).GetMetadata()

	testUpdateClusterView(ctx, re, metadata)
	testRegisterNode(ctx, re, metadata)
	testTableOperation(ctx, re, metadata)
	testShardOperation(ctx, re, metadata)
	testMetadataOperation(ctx, re, metadata)
}

func testUpdateClusterView(ctx context.Context, re *require.Assertions, m *metadata.ClusterMetadata) {
	// Remove a shard on node.
	currentShardNodes := m.GetClusterSnapshot().Topology.ClusterView.ShardNodes
	removeTarget := currentShardNodes[0]
	newShardNodes := make(map[string][]storage.ShardNode)
	for i := 1; i < len(currentShardNodes); i++ {
		if removeTarget.NodeName == currentShardNodes[i].NodeName {
			if removeTarget.ID != currentShardNodes[i].ID {
				newShardNodes[currentShardNodes[i].NodeName] = append(newShardNodes[currentShardNodes[i].NodeName], currentShardNodes[i])
			}
		}
	}
	err := m.UpdateClusterViewByNode(ctx, newShardNodes)
	re.NoError(err)
	// New topology shard not contains the target shardNode.
	for _, shardNode := range m.GetClusterSnapshot().Topology.ClusterView.ShardNodes {
		re.NotEqual(removeTarget.ID, shardNode.ID)
	}
	re.Equal(len(currentShardNodes)-1, len(m.GetClusterSnapshot().Topology.ClusterView.ShardNodes))

	// Update cluster state and reset shardNodes.
	err = m.UpdateClusterView(ctx, storage.ClusterStateEmpty, currentShardNodes)
	re.NoError(err)
	re.Equal(storage.ClusterStateEmpty, m.GetClusterState())
	re.Equal(len(currentShardNodes), len(m.GetClusterSnapshot().Topology.ClusterView.ShardNodes))
}

func testRegisterNode(ctx context.Context, re *require.Assertions, m *metadata.ClusterMetadata) {
	currentShardNodes := m.GetClusterSnapshot().Topology.ClusterView.ShardNodes
	currentRegisterNodes := m.GetRegisteredNodes()
	// Register node with empty shard.
	newNodeName := "testRegisterNode"
	lastTouchTime := uint64(time.Now().UnixMilli())
	err := m.RegisterNode(ctx, metadata.RegisteredNode{
		Node: storage.Node{
			Name:          newNodeName,
			NodeStats:     storage.NodeStats{},
			LastTouchTime: lastTouchTime,
			State:         0,
		},
		ShardInfos: nil,
	})
	re.NoError(err)
	re.Equal(len(currentRegisterNodes)+1, len(m.GetRegisteredNodes()))
	node, exists := m.GetRegisteredNodeByName(newNodeName)
	re.True(exists)
	re.Equal(lastTouchTime, node.Node.LastTouchTime)

	// Update lastTouchTime.
	lastTouchTime = uint64(time.Now().UnixMilli())
	node.Node.LastTouchTime = lastTouchTime
	err = m.RegisterNode(ctx, node)
	re.NoError(err)
	re.Equal(len(currentRegisterNodes)+1, len(m.GetRegisteredNodes()))
	node, exists = m.GetRegisteredNodeByName(newNodeName)
	re.True(exists)
	re.Equal(lastTouchTime, node.Node.LastTouchTime)

	// Reset shardNodes.
	err = m.UpdateClusterView(ctx, storage.ClusterStateStable, currentShardNodes)
	re.NoError(err)
	re.Equal(len(currentShardNodes), len(m.GetClusterSnapshot().Topology.ClusterView.ShardNodes))
}

func testTableOperation(ctx context.Context, re *require.Assertions, m *metadata.ClusterMetadata) {
	testSchema := "testSchemaName"
	testTableName := "testTableName0"
	// Test create schema.
	schema, _, err := m.GetOrCreateSchema(ctx, testSchema)
	re.NoError(err)
	re.Equal(testSchema, schema.Name)

	// Test create table metadata.
	createMetadataResult, err := m.CreateTableMetadata(ctx, metadata.CreateTableMetadataRequest{
		SchemaName:    testSchema,
		TableName:     testTableName,
		PartitionInfo: storage.PartitionInfo{},
	})
	re.NoError(err)
	re.Equal(createMetadataResult.Table.Name, testTableName)

	// Table metadata is exists.
	t, exists, err := m.GetTable(testSchema, testTableName)
	re.NoError(err)
	re.True(exists)
	re.Equal(testTableName, t.Name)

	// Route table should return error when table metadata is not exists in any shard.
	_, err = m.RouteTables(ctx, testSchema, []string{testTableName})
	re.Error(err)

	// Test drop table metadata.
	dropMetadataResult, err := m.DropTableMetadata(ctx, testSchema, testTableName)
	re.NoError(err)
	re.Equal(testTableName, dropMetadataResult.Table.Name)

	// Table metadata is not exists.
	t, exists, err = m.GetTable(testSchema, testTableName)
	re.NoError(err)
	re.False(exists)
	re.False(exists)

	// Test create table.
	createResult, err := m.CreateTable(ctx, metadata.CreateTableRequest{
		ShardID:       0,
		SchemaName:    testSchema,
		TableName:     testTableName,
		PartitionInfo: storage.PartitionInfo{},
	})
	re.NoError(err)
	re.Equal(testTableName, createResult.Table.Name)

	// Test route table, it should return shardNode.
	routeResult, err := m.RouteTables(ctx, testSchema, []string{testTableName})
	re.NoError(err)
	re.Equal(1, len(routeResult.RouteEntries))

	// Migrate this table to another shard.
	err = m.MigrateTable(ctx, metadata.MigrateTableRequest{
		SchemaName: testSchema,
		TableNames: []string{testTableName},
		OldShardID: 0,
		NewShardID: 1,
	})
	re.NoError(err)

	// Check migrate result, route table should return another shard.
	routeResult, err = m.RouteTables(ctx, testSchema, []string{testTableName})
	re.NoError(err)
	re.Equal(1, len(routeResult.RouteEntries))
	re.Equal(storage.ShardID(1), routeResult.RouteEntries[testTableName].NodeShards[0].ShardInfo.ID)

	// Drop table already created.
	dropResult, err := m.DropTable(ctx, testSchema, testTableName)
	re.NoError(err)
	re.Equal(storage.ShardID(1), dropResult.ShardVersionUpdate[0].ShardID)
}

func testShardOperation(ctx context.Context, re *require.Assertions, m *metadata.ClusterMetadata) {
	newID, err := m.AllocShardID(ctx)
	re.NoError(err)

	err = m.CreateShardViews(ctx, []metadata.CreateShardView{{
		ShardID: storage.ShardID(newID),
		Tables:  nil,
	}})
	re.NoError(err)

	shardNodeResult, err := m.GetNodeShards(ctx)
	re.NoError(err)

	shardNodes, err := m.GetShardNodesByShardID(shardNodeResult.NodeShards[0].ShardInfo.ID)
	re.NoError(err)
	re.Equal(1, len(shardNodes))

	shardTables := m.GetShardTables([]storage.ShardID{shardNodeResult.NodeShards[0].ShardInfo.ID})
	re.Equal(1, len(shardTables))

	_, err = m.GetShardNodeByTableIDs([]storage.TableID{})
	re.NoError(err)

	err = m.DropShardNode(ctx, []storage.ShardNode{{
		ID:        shardNodeResult.NodeShards[0].ShardNode.ID,
		ShardRole: shardNodeResult.NodeShards[0].ShardNode.ShardRole,
		NodeName:  shardNodeResult.NodeShards[0].ShardNode.NodeName,
	}})
	re.NoError(err)
}

func testMetadataOperation(ctx context.Context, re *require.Assertions, m *metadata.ClusterMetadata) {
	// Init cluster metadata, it will throw error because it has been init.
	err := m.Init(ctx)
	re.Error(err)

	err = m.Load(ctx)
	re.NoError(err)

	// Load metadata from storage, it will throw error because it is not persisted。
	err = m.LoadMetadata(ctx)
	re.Error(err)
}
