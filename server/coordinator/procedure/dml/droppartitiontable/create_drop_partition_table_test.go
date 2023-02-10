// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package droppartitiontable_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/CeresDB/ceresdbproto/golang/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/dml/createpartitiontable"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/dml/droppartitiontable"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/operation/scatter"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateAndDropPartitionTable(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := test.MockDispatch{}
	manager, c := scatter.Prepare(t)
	s := test.NewTestStorage(t)

	shardPicker := coordinator.NewRandomBalancedShardPicker(manager)

	testTableNum := 8
	testSubTableNum := 4

	// Create table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", test.TestTableName0, i)
		subTableNames := genSubTables(tableName, testSubTableNum)
		testCreatePartitionTable(ctx, t, dispatch, c, s, shardPicker, tableName, subTableNames)
	}

	// Check get table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", test.TestTableName0, i)
		table := checkTable(t, c, tableName, true)
		re.Equal(table.PartitionInfo.Info != nil, true)
		subTableNames := genSubTables(tableName, testSubTableNum)
		for _, subTableName := range subTableNames {
			checkTable(t, c, subTableName, true)
		}
	}

	// Drop table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", test.TestTableName0, i)
		subTableNames := genSubTables(tableName, testSubTableNum)
		testDropPartitionTable(t, dispatch, c, s, tableName, subTableNames)
	}

	// Check table not exists.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", test.TestTableName0, i)
		checkTable(t, c, tableName, false)
		subTableNames := genSubTables(tableName, testSubTableNum)
		for _, subTableName := range subTableNames {
			checkTable(t, c, subTableName, false)
		}
	}
}

func testCreatePartitionTable(ctx context.Context, t *testing.T, dispatch eventdispatch.Dispatch, c *cluster.Cluster, s procedure.Storage, shardPicker coordinator.ShardPicker, tableName string, subTableNames []string) {
	re := require.New(t)

	request := &metaservicepb.CreateTableRequest{
		Header: &metaservicepb.RequestHeader{
			Node:        test.NodeName0,
			ClusterName: test.ClusterName,
		},
		PartitionTableInfo: &metaservicepb.PartitionTableInfo{
			SubTableNames: subTableNames,
			PartitionInfo: &clusterpb.PartitionInfo{},
		},
		SchemaName: test.TestSchemaName,
		Name:       tableName,
	}

	getNodeShardResult, err := c.GetNodeShards(ctx)
	re.NoError(err)

	nodeNames := make(map[string]int)
	for _, nodeShard := range getNodeShardResult.NodeShards {
		nodeNames[nodeShard.ShardNode.NodeName] = 1
	}

	partitionTableNum := procedure.Max(1, int(float32(len(nodeNames))*test.DefaultPartitionTableProportionOfNodes))

	partitionTableShards, err := shardPicker.PickShards(ctx, c.Name(), partitionTableNum, true)
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

func testDropPartitionTable(t *testing.T, dispatch eventdispatch.Dispatch, c *cluster.Cluster, s procedure.Storage, tableName string, subTableNames []string) {
	re := require.New(t)
	// New DropPartitionTableProcedure to drop table.
	req := droppartitiontable.ProcedureRequest{
		ID: uint64(1), Dispatch: dispatch, Cluster: c, Request: &metaservicepb.DropTableRequest{
			Header: &metaservicepb.RequestHeader{
				Node:        test.NodeName0,
				ClusterName: test.ClusterName,
			},
			SchemaName:         test.TestSchemaName,
			Name:               tableName,
			PartitionTableInfo: &metaservicepb.PartitionTableInfo{SubTableNames: subTableNames},
		}, OnSucceeded: func(_ cluster.TableInfo) error {
			return nil
		}, OnFailed: func(_ error) error {
			return nil
		}, Storage: s,
	}

	procedure := droppartitiontable.NewProcedure(req)
	err := procedure.Start(context.Background())
	re.NoError(err)
}

func genSubTables(tableName string, tableNum int) []string {
	var subTableNames []string
	for j := 0; j < tableNum; j++ {
		subTableNames = append(subTableNames, fmt.Sprintf("%s_%d", tableName, j))
	}
	return subTableNames
}

func checkTable(t *testing.T, c *cluster.Cluster, tableName string, exist bool) storage.Table {
	re := require.New(t)
	table, b, err := c.GetTable(test.TestSchemaName, tableName)
	re.NoError(err)
	re.Equal(b, exist)
	return table
}
