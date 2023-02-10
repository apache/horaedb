// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package createpartitiontable_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/dml/createpartitiontable"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/operation/scatter"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/stretchr/testify/require"
)

func TestCreatePartitionTable(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := test.MockDispatch{}
	manager, c := scatter.Prepare(t)
	s := test.NewTestStorage(t)

	shardPicker := coordinator.NewRandomBalancedShardPicker(manager)

	request := &metaservicepb.CreateTableRequest{
		Header: &metaservicepb.RequestHeader{
			Node:        test.NodeName0,
			ClusterName: test.ClusterName,
		},
		PartitionTableInfo: &metaservicepb.PartitionTableInfo{
			SubTableNames: []string{"p1", "p2"},
		},
		SchemaName: test.TestSchemaName,
		Name:       test.TestTableName0,
	}

	getNodeShardResult, err := c.GetNodeShards(ctx)
	re.NoError(err)

	nodeNames := make(map[string]int)
	for _, nodeShard := range getNodeShardResult.NodeShards {
		nodeNames[nodeShard.ShardNode.NodeName] = 1
	}

	partitionTableNum := procedure.Max(1, int(float32(len(nodeNames))*test.DefaultPartitionTableProportionOfNodes))

	partitionTableShards, err := shardPicker.PickShards(ctx, c.Name(), partitionTableNum, false)
	re.NoError(err)
	dataTableShards, err := shardPicker.PickShards(ctx, c.Name(), len(request.GetPartitionTableInfo().SubTableNames), true)
	re.NoError(err)

	procedure := createpartitiontable.NewProcedure(createpartitiontable.ProcedureRequest{
		ID: 1, Cluster: c, Dispatch: dispatch, Storage: s, Req: request, PartitionTableShards: partitionTableShards, SubTablesShards: dataTableShards, OnSucceeded: func(_ cluster.CreateTableResult) error {
			return nil
		}, OnFailed: func(_ error) error {
			return nil
		},
	})

	err = procedure.Start(ctx)
	re.NoError(err)
}
