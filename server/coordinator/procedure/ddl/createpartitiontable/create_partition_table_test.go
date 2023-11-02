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

package createpartitiontable_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/ddl/createpartitiontable"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestCreatePartitionTable(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := test.MockDispatch{}
	s := test.NewTestStorage(t)
	c := test.InitStableCluster(ctx, t)

	shardNode := c.GetMetadata().GetClusterSnapshot().Topology.ClusterView.ShardNodes[0]

	request := &metaservicepb.CreateTableRequest{
		Header: &metaservicepb.RequestHeader{
			Node:        shardNode.NodeName,
			ClusterName: test.ClusterName,
		},
		PartitionTableInfo: &metaservicepb.PartitionTableInfo{
			SubTableNames: []string{"p1", "p2"},
		},
		SchemaName: test.TestSchemaName,
		Name:       test.TestTableName0,
	}

	shardPicker := coordinator.NewLeastTableShardPicker()
	subTableShards, err := shardPicker.PickShards(ctx, c.GetMetadata().GetClusterSnapshot(), len(request.GetPartitionTableInfo().SubTableNames))

	shardNodesWithVersion := make([]metadata.ShardNodeWithVersion, 0, len(subTableShards))
	for _, subTableShard := range subTableShards {
		shardView, exists := c.GetMetadata().GetClusterSnapshot().Topology.ShardViewsMapping[subTableShard.ID]
		re.True(exists)
		shardNodesWithVersion = append(shardNodesWithVersion, metadata.ShardNodeWithVersion{
			ShardInfo: metadata.ShardInfo{
				ID:      shardView.ShardID,
				Role:    subTableShard.ShardRole,
				Version: shardView.Version,
				Status:  storage.ShardStatusUnknown,
			},
			ShardNode: subTableShard,
		})
	}

	re.NoError(err)
	procedure, err := createpartitiontable.NewProcedure(createpartitiontable.ProcedureParams{
		ID:              0,
		ClusterMetadata: c.GetMetadata(),
		ClusterSnapshot: c.GetMetadata().GetClusterSnapshot(),
		Dispatch:        dispatch,
		Storage:         s,
		SourceReq:       request,
		SubTablesShards: shardNodesWithVersion,
		OnSucceeded: func(result metadata.CreateTableResult) error {
			return nil
		},
		OnFailed: func(err error) error {
			return nil
		},
	})
	re.NoError(err)

	err = procedure.Start(ctx)
	re.NoError(err)
}
