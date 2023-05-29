// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package metadata_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	TestMinShardID = 0
	TestShardID    = 0
	TestShardID1   = 1
	TestShardID2   = 2
	TestNodeName   = "TestNodeName"
	TestNodeName1  = "TestNodeName1"
	TestTableID    = 0
	TestSchemaID   = 0
)

func TestTopologyManager(t *testing.T) {
	ctx := context.Background()
	re := require.New(t)

	_, client, _ := etcdutil.PrepareEtcdServerAndClient(t)
	clusterStorage := storage.NewStorageWithEtcdBackend(client, TestRootPath, storage.Options{
		MaxScanLimit: 100, MinScanLimit: 10,
	})
	shardIDAlloc := id.NewReusableAllocatorImpl([]uint64{}, TestMinShardID)

	topologyManager := metadata.NewTopologyManagerImpl(zap.NewNop(), clusterStorage, TestClusterID, shardIDAlloc)

	err := topologyManager.InitClusterView(ctx)
	re.NoError(err)
	err = topologyManager.CreateShardViews(ctx, []metadata.CreateShardView{
		{
			ShardID: TestShardID,
			Tables:  nil,
		},
	})
	re.NoError(err)

	testTableTopology(ctx, re, topologyManager)
	testShardTopology(ctx, re, topologyManager)
}

func testTableTopology(ctx context.Context, re *require.Assertions, manager metadata.TopologyManager) {
	updateVersionResult, err := manager.AddTable(ctx, TestShardID, []storage.Table{{
		ID:            TestTableID,
		Name:          TestTableName,
		SchemaID:      TestSchemaID,
		CreatedAt:     0,
		PartitionInfo: storage.PartitionInfo{},
	}})
	re.NoError(err)
	re.Equal(updateVersionResult.PrevVersion, updateVersionResult.CurrVersion-1)

	shardTables := manager.GetTableIDs([]storage.ShardID{updateVersionResult.ShardID})
	found := foundTable(TestTableID, shardTables, updateVersionResult)
	re.Equal(true, found)

	evictVersionResult, err := manager.EvictTable(ctx, TestTableID)
	re.NoError(err)
	re.Equal(1, len(evictVersionResult))
	re.Equal(updateVersionResult.ShardID, evictVersionResult[0].ShardID)
	re.Equal(updateVersionResult.CurrVersion, evictVersionResult[0].PrevVersion)

	shardTables = manager.GetTableIDs([]storage.ShardID{updateVersionResult.ShardID})
	found = foundTable(TestTableID, shardTables, updateVersionResult)
	re.Equal(false, found)

	updateVersionResult, err = manager.AddTable(ctx, TestShardID, []storage.Table{{
		ID:            TestTableID,
		Name:          TestTableName,
		SchemaID:      TestSchemaID,
		CreatedAt:     0,
		PartitionInfo: storage.PartitionInfo{},
	}})
	re.NoError(err)
	re.Equal(updateVersionResult.PrevVersion, updateVersionResult.CurrVersion-1)

	shardTables = manager.GetTableIDs([]storage.ShardID{updateVersionResult.ShardID})
	found = foundTable(TestTableID, shardTables, updateVersionResult)
	re.Equal(true, found)

	updateVersionResult, err = manager.RemoveTable(ctx, updateVersionResult.ShardID, []storage.TableID{TestTableID})
	re.NoError(err)
	re.Equal(updateVersionResult.PrevVersion, updateVersionResult.CurrVersion-1)
}

func foundTable(targetTableID storage.TableID, shardTables map[storage.ShardID]metadata.ShardTableIDs, updateVersionResult metadata.ShardVersionUpdate) bool {
	tableIDs := shardTables[updateVersionResult.ShardID].TableIDs
	for _, tableID := range tableIDs {
		if tableID == targetTableID {
			return true
		}
	}
	return false
}

func testShardTopology(ctx context.Context, re *require.Assertions, manager metadata.TopologyManager) {
	var shardNodes []storage.ShardNode
	shardNodes = append(shardNodes, storage.ShardNode{
		ID:        TestShardID1,
		ShardRole: 0,
		NodeName:  TestNodeName,
	})
	shardNodes = append(shardNodes, storage.ShardNode{
		ID:        TestShardID2,
		ShardRole: 0,
		NodeName:  TestNodeName1,
	})

	err := manager.UpdateClusterView(ctx, storage.ClusterStateStable, shardNodes)
	re.NoError(err)
	re.Equal(2, len(manager.GetTopology().ClusterView.ShardNodes))

	shardNodeMapping := map[string][]storage.ShardNode{}
	shardNodeMapping[TestNodeName] = []storage.ShardNode{}
	err = manager.UpdateClusterViewByNode(ctx, shardNodeMapping)
	re.NoError(err)
	re.Equal(1, len(manager.GetTopology().ClusterView.ShardNodes))

	err = manager.DropShardNodes(ctx, []storage.ShardNode{
		{
			ID:        TestShardID2,
			ShardRole: 0,
			NodeName:  TestNodeName1,
		},
	})
	re.NoError(err)
	re.Equal(0, len(manager.GetTopology().ClusterView.ShardNodes))
}
