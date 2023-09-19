// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package droppartitiontable_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/CeresDB/ceresdbproto/golang/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/ddl/createpartitiontable"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/ddl/droppartitiontable"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateAndDropPartitionTable(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := test.MockDispatch{}
	c := test.InitStableCluster(ctx, t)
	s := test.NewTestStorage(t)

	shardNode := c.GetMetadata().GetClusterSnapshot().Topology.ClusterView.ShardNodes[0]

	shardPicker := coordinator.NewRandomBalancedShardPicker()

	testTableNum := 8
	testSubTableNum := 4

	// Create table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", test.TestTableName0, i)
		subTableNames := genSubTables(tableName, testSubTableNum)
		testCreatePartitionTable(ctx, t, dispatch, c, s, shardPicker, shardNode.NodeName, tableName, subTableNames)
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
		testDropPartitionTable(t, dispatch, c, s, shardNode.NodeName, tableName, subTableNames)
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

func testCreatePartitionTable(ctx context.Context, t *testing.T, dispatch eventdispatch.Dispatch, c *cluster.Cluster, s procedure.Storage, shardPicker coordinator.ShardPicker, nodeName string, tableName string, subTableNames []string) {
	re := require.New(t)

	request := &metaservicepb.CreateTableRequest{
		Header: &metaservicepb.RequestHeader{
			Node:        nodeName,
			ClusterName: test.ClusterName,
		},
		PartitionTableInfo: &metaservicepb.PartitionTableInfo{
			SubTableNames: subTableNames,
			PartitionInfo: &clusterpb.PartitionInfo{},
		},
		SchemaName: test.TestSchemaName,
		Name:       tableName,
	}

	subTableShards, err := shardPicker.PickShards(ctx, c.GetMetadata().GetClusterSnapshot(), len(request.GetPartitionTableInfo().SubTableNames))
	re.NoError(err)

	shardNodesWithVersion := make([]metadata.ShardNodeWithVersion, 0, len(subTableShards))
	for _, subTableShard := range subTableShards {
		shardView, exists := c.GetMetadata().GetClusterSnapshot().Topology.ShardViewsMapping[subTableShard.ID]
		re.True(exists)
		shardNodesWithVersion = append(shardNodesWithVersion, metadata.ShardNodeWithVersion{
			ShardInfo: metadata.ShardInfo{
				ID:      shardView.ShardID,
				Role:    subTableShard.ShardRole,
				Version: shardView.Version,
			},
			ShardNode: subTableShard,
		})
	}

	procedure, err := createpartitiontable.NewProcedure(createpartitiontable.ProcedureParams{
		ID:              0,
		ClusterMetadata: c.GetMetadata(),
		ClusterSnapshot: c.GetMetadata().GetClusterSnapshot(),
		Dispatch:        dispatch,
		Storage:         s,
		SourceReq:       request,
		SubTablesShards: shardNodesWithVersion,
		OnSucceeded: func(_ metadata.CreateTableResult) error {
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

func testDropPartitionTable(t *testing.T, dispatch eventdispatch.Dispatch, c *cluster.Cluster, s procedure.Storage, nodeName string, tableName string, subTableNames []string) {
	re := require.New(t)
	// New DropPartitionTableProcedure to drop table.
	req := droppartitiontable.ProcedureParams{
		ID: uint64(1), Dispatch: dispatch, ClusterMetadata: c.GetMetadata(), ClusterSnapshot: c.GetMetadata().GetClusterSnapshot(), SourceReq: &metaservicepb.DropTableRequest{
			Header: &metaservicepb.RequestHeader{
				Node:        nodeName,
				ClusterName: test.ClusterName,
			},
			SchemaName:         test.TestSchemaName,
			Name:               tableName,
			PartitionTableInfo: &metaservicepb.PartitionTableInfo{SubTableNames: subTableNames},
		}, OnSucceeded: func(_ metadata.TableInfo) error {
			return nil
		}, OnFailed: func(_ error) error {
			return nil
		}, Storage: s,
	}

	procedure, ok, err := droppartitiontable.NewProcedure(req)
	re.NoError(err)
	re.True(ok)
	err = procedure.Start(context.Background())
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
	table, b, err := c.GetMetadata().GetTable(test.TestSchemaName, tableName)
	re.NoError(err)
	re.Equal(b, exist)
	return table
}
