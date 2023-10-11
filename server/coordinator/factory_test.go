// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
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
	re.Equal(procedure.CreateTable, p.Typ())
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
	re.Equal(procedure.CreatePartitionTable, p.Typ())
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
	re.Equal(procedure.TransferLeader, p.Typ())
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
	re.Equal(procedure.Split, p.Typ())
	re.Equal(procedure.StateInit, string(p.State()))
}
