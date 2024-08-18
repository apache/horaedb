/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package split_test

import (
	"context"
	"testing"

	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/operation/split"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/test"
	"github.com/apache/incubator-horaedb-meta/server/storage"
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
		ShardID:       createTableNodeShard.ID,
		LatestVersion: 0,
		SchemaName:    test.TestSchemaName,
		TableName:     test.TestTableName0,
		PartitionInfo: storage.PartitionInfo{Info: nil},
	})
	re.NoError(err)
	_, err = c.GetMetadata().CreateTable(ctx, metadata.CreateTableRequest{
		ShardID:       createTableNodeShard.ID,
		LatestVersion: 0,
		SchemaName:    test.TestSchemaName,
		TableName:     test.TestTableName1,
		PartitionInfo: storage.PartitionInfo{Info: nil},
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
