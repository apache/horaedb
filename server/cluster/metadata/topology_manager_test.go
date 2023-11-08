/*
 * Copyright 2022 The CeresDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
		MaxScanLimit: 100, MinScanLimit: 10, MaxOpsPerTxn: 32,
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
	err := manager.AddTable(ctx, TestShardID, 0, []storage.Table{{
		ID:            TestTableID,
		Name:          TestTableName,
		SchemaID:      TestSchemaID,
		CreatedAt:     0,
		PartitionInfo: storage.PartitionInfo{Info: nil},
	}})
	re.NoError(err)

	shardTables := manager.GetTableIDs([]storage.ShardID{TestShardID})
	found := foundTable(TestTableID, shardTables, TestTableID)
	re.Equal(true, found)

	err = manager.RemoveTable(ctx, TestShardID, 0, []storage.TableID{TestTableID})
	re.NoError(err)

	shardTables = manager.GetTableIDs([]storage.ShardID{TestTableID})
	found = foundTable(TestTableID, shardTables, TestTableID)
	re.Equal(false, found)

	err = manager.AddTable(ctx, TestShardID, 0, []storage.Table{{
		ID:            TestTableID,
		Name:          TestTableName,
		SchemaID:      TestSchemaID,
		CreatedAt:     0,
		PartitionInfo: storage.PartitionInfo{Info: nil},
	}})
	re.NoError(err)

	shardTables = manager.GetTableIDs([]storage.ShardID{TestTableID})
	found = foundTable(TestTableID, shardTables, TestTableID)
	re.Equal(true, found)

	err = manager.RemoveTable(ctx, TestTableID, 0, []storage.TableID{TestTableID})
	re.NoError(err)
}

func foundTable(targetTableID storage.TableID, shardTables map[storage.ShardID]metadata.ShardTableIDs, shardID storage.ShardID) bool {
	tableIDs := shardTables[shardID].TableIDs
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
