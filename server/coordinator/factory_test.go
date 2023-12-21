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

package coordinator_test

import (
	"context"
	"testing"

	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/test"
	"github.com/apache/incubator-horaedb-proto/golang/pkg/metaservicepb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func setupFactory(t *testing.T) (*coordinator.Factory, *metadata.ClusterMetadata) {
	ctx := context.Background()
	c := test.InitStableCluster(ctx, t)

	dispatch := test.MockDispatch{}
	allocator := test.MockIDAllocator{}
	storage := test.NewTestStorage(t)
	f := coordinator.NewFactory(zap.NewNop(), allocator, dispatch, storage)

	return f, c.GetMetadata()
}

func TestCreateTable(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	f, m := setupFactory(t)
	// Create normal table procedure.
	p, err := f.MakeCreateTableProcedure(ctx, coordinator.CreateTableRequest{
		ClusterMetadata: m,
		SourceReq: &metaservicepb.CreateTableRequest{
			Header:             nil,
			SchemaName:         test.TestSchemaName,
			Name:               "test1",
			EncodedSchema:      nil,
			Engine:             "",
			CreateIfNotExist:   false,
			Options:            nil,
			PartitionTableInfo: nil,
		},
		OnSucceeded: nil,
		OnFailed:    nil,
	})
	re.NoError(err)
	re.Equal(procedure.CreateTable, p.Kind())
	re.Equal(procedure.StateInit, string(p.State()))

	// Create partition table procedure.
	p, err = f.MakeCreateTableProcedure(ctx, coordinator.CreateTableRequest{
		ClusterMetadata: m,
		SourceReq: &metaservicepb.CreateTableRequest{
			Header:           nil,
			SchemaName:       test.TestSchemaName,
			Name:             "test2",
			EncodedSchema:    nil,
			Engine:           "",
			CreateIfNotExist: false,
			Options:          nil,
			PartitionTableInfo: &metaservicepb.PartitionTableInfo{
				PartitionInfo: nil,
				SubTableNames: []string{"test2-0,test2-1"},
			},
		},
		OnSucceeded: nil,
		OnFailed:    nil,
	})
	re.NoError(err)
	re.Equal(procedure.CreatePartitionTable, p.Kind())
	re.Equal(procedure.StateInit, string(p.State()))
}

func TestDropTable(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	f, m := setupFactory(t)
	// Drop normal table procedure.
	p, ok, err := f.CreateDropTableProcedure(ctx, coordinator.DropTableRequest{
		ClusterMetadata: m,
		ClusterSnapshot: m.GetClusterSnapshot(),
		SourceReq: &metaservicepb.DropTableRequest{
			Header:             nil,
			SchemaName:         test.TestSchemaName,
			Name:               "test1",
			PartitionTableInfo: nil,
		},
		OnSucceeded: nil,
		OnFailed:    nil,
	})
	re.NoError(err)
	re.False(ok)
	re.Nil(p)

	// Create partition table procedure.
	p, ok, err = f.CreateDropTableProcedure(ctx, coordinator.DropTableRequest{
		ClusterMetadata: m,
		ClusterSnapshot: m.GetClusterSnapshot(),
		SourceReq: &metaservicepb.DropTableRequest{
			Header:     nil,
			SchemaName: test.TestSchemaName,
			Name:       "test2",
			PartitionTableInfo: &metaservicepb.PartitionTableInfo{
				PartitionInfo: nil,
				SubTableNames: []string{"test2-0,test2-1"},
			},
		},
		OnSucceeded: nil,
		OnFailed:    nil,
	})
	// Drop non-existing partition table.
	re.NoError(err)
	re.True(ok)
	re.NotNil(p)
}

func TestTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	f, m := setupFactory(t)
	snapshot := m.GetClusterSnapshot()
	p, err := f.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
		Snapshot:          snapshot,
		ShardID:           0,
		OldLeaderNodeName: "",
		NewLeaderNodeName: snapshot.RegisteredNodes[0].Node.Name,
	})
	re.NoError(err)
	re.Equal(procedure.TransferLeader, p.Kind())
	re.Equal(procedure.StateInit, string(p.State()))
}

func TestSplit(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	f, m := setupFactory(t)
	snapshot := m.GetClusterSnapshot()
	p, err := f.CreateSplitProcedure(ctx, coordinator.SplitRequest{
		ClusterMetadata: nil,
		SchemaName:      "",
		TableNames:      nil,
		Snapshot:        snapshot,
		ShardID:         snapshot.Topology.ClusterView.ShardNodes[0].ID,
		NewShardID:      100,
		TargetNodeName:  snapshot.Topology.ClusterView.ShardNodes[0].NodeName,
	})
	re.NoError(err)
	re.Equal(procedure.Split, p.Kind())
	re.Equal(procedure.StateInit, string(p.State()))
}
