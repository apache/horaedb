// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"fmt"
	"testing"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/stretchr/testify/require"
)

func TestCreateAndDropPartitionTable(t *testing.T) {
	ctx := context.Background()
	dispatch := MockDispatch{}
	manager, c := prepare(t)
	s := NewTestStorage(t)

	shardPicker := NewRandomBalancedShardPicker(manager)

	testTableNum := 20
	testSubTableNum := 4

	// Create table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", testTableName0, i)
		subTableNames := genSubTables(tableName, testSubTableNum)
		testCreatePartitionTable(ctx, t, dispatch, c, s, shardPicker, tableName, subTableNames)
	}

	// Check get table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", testTableName0, i)
		checkTable(t, c, tableName, true)
		subTableNames := genSubTables(tableName, testSubTableNum)
		for _, subTableName := range subTableNames {
			checkTable(t, c, subTableName, true)
		}
	}

	// Drop table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", testTableName0, i)
		subTableNames := genSubTables(tableName, testSubTableNum)
		testDropPartitionTable(t, dispatch, c, s, tableName, subTableNames)
	}

	// Check table not exists.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", testTableName0, i)
		checkTable(t, c, tableName, false)
		subTableNames := genSubTables(tableName, testSubTableNum)
		for _, subTableName := range subTableNames {
			checkTable(t, c, subTableName, false)
		}
	}
}

func testCreatePartitionTable(ctx context.Context, t *testing.T, dispatch eventdispatch.Dispatch, c *cluster.Cluster, s Storage, shardPicker ShardPicker, tableName string, subTableNames []string) {
	re := require.New(t)

	request := &metaservicepb.CreateTableRequest{
		Header: &metaservicepb.RequestHeader{
			Node:        nodeName0,
			ClusterName: clusterName,
		},
		PartitionTableInfo: &metaservicepb.PartitionTableInfo{
			SubTableNames: subTableNames,
		},
		SchemaName: testSchemaName,
		Name:       tableName,
	}

	getNodeShardResult, err := c.GetNodeShards(ctx)
	re.NoError(err)

	nodeNames := make(map[string]int)
	for _, nodeShard := range getNodeShardResult.NodeShards {
		nodeNames[nodeShard.ShardNode.NodeName] = 1
	}

	partitionTableNum := Max(1, int(float32(len(nodeNames))*defaultPartitionTableProportionOfNodes))

	partitionTableShards, err := shardPicker.PickShards(ctx, c.Name(), partitionTableNum, true)
	re.NoError(err)
	dataTableShards, err := shardPicker.PickShards(ctx, c.Name(), len(request.GetPartitionTableInfo().SubTableNames), true)
	re.NoError(err)

	procedure := NewCreatePartitionTableProcedure(CreatePartitionTableProcedureRequest{
		1, c, dispatch, s, request, partitionTableShards, dataTableShards, func(_ cluster.CreateTableResult) error {
			return nil
		}, func(_ error) error {
			return nil
		},
	})

	err = procedure.Start(ctx)
	re.NoError(err)
}

func testDropPartitionTable(t *testing.T, dispatch eventdispatch.Dispatch, c *cluster.Cluster, s Storage, tableName string, subTableNames []string) {
	re := require.New(t)
	// New DropPartitionTableProcedure to drop table.
	req := DropPartitionTableProcedureRequest{
		ID: uint64(1), Dispatch: dispatch, Cluster: c, Request: &metaservicepb.DropTableRequest{
			Header: &metaservicepb.RequestHeader{
				Node:        nodeName0,
				ClusterName: clusterName,
			},
			SchemaName:         testSchemaName,
			Name:               tableName,
			PartitionTableInfo: &metaservicepb.PartitionTableInfo{SubTableNames: subTableNames},
		}, OnSucceeded: func(_ cluster.TableInfo) error {
			return nil
		}, OnFailed: func(_ error) error {
			return nil
		}, Storage: s,
	}

	procedure := NewDropPartitionTableProcedure(req)
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

func checkTable(t *testing.T, c *cluster.Cluster, tableName string, exist bool) {
	re := require.New(t)
	_, b, err := c.GetTable(testSchemaName, tableName)
	re.NoError(err)
	re.Equal(b, exist)
}
