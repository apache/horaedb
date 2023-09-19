// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package droptable_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/ddl/createtable"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/ddl/droptable"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateAndDropTable(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := test.MockDispatch{}
	c := test.InitStableCluster(ctx, t)

	nodeName := c.GetMetadata().GetClusterSnapshot().Topology.ClusterView.ShardNodes[0].NodeName
	shardID := c.GetMetadata().GetClusterSnapshot().Topology.ClusterView.ShardNodes[0].ID

	testTableNum := 20
	// Create table.
	for i := 0; i < testTableNum; i++ {
		// Select a shard to open table.
		snapshot := c.GetMetadata().GetClusterSnapshot()
		tableName := fmt.Sprintf("%s_%d", test.TestTableName0, i)
		testCreateTable(t, dispatch, c, snapshot, shardID, nodeName, tableName)
	}
	// Check get table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", test.TestTableName0, i)
		table, b, err := c.GetMetadata().GetTable(test.TestSchemaName, tableName)
		re.NoError(err)
		re.Equal(b, true)
		re.NotNil(table)
	}

	// Check tables by node.
	var shardIDs []storage.ShardID
	for i := 0; i < test.DefaultShardTotal; i++ {
		shardIDs = append(shardIDs, storage.ShardID(i))
	}
	shardTables := c.GetMetadata().GetShardTables(shardIDs)
	tableTotal := 0
	for _, v := range shardTables {
		tableTotal += len(v.Tables)
	}
	re.Equal(testTableNum, tableTotal)

	// Drop table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", test.TestTableName0, i)
		testDropTable(t, dispatch, c, nodeName, tableName)
	}
	// Check table not exists.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", test.TestTableName0, i)
		_, b, err := c.GetMetadata().GetTable(test.TestSchemaName, tableName)
		re.NoError(err)
		re.Equal(b, false)
	}

	// Check tables by node.
	shardTables = c.GetMetadata().GetShardTables(shardIDs)
	tableTotal = 0
	for _, v := range shardTables {
		tableTotal += len(v.Tables)
	}
	re.Equal(tableTotal, 0)
}

func testCreateTable(t *testing.T, dispatch eventdispatch.Dispatch, c *cluster.Cluster, snapshot metadata.Snapshot, shardID storage.ShardID, nodeName, tableName string) {
	re := require.New(t)
	// New CreateTableProcedure to create a new table.
	p, err := createtable.NewProcedure(createtable.ProcedureParams{
		Dispatch:        dispatch,
		ClusterMetadata: c.GetMetadata(),
		ClusterSnapshot: snapshot,
		ID:              uint64(1),
		ShardID:         shardID,
		SourceReq: &metaservicepb.CreateTableRequest{
			Header: &metaservicepb.RequestHeader{
				Node:        nodeName,
				ClusterName: test.ClusterName,
			},
			SchemaName: test.TestSchemaName,
			Name:       tableName,
		},
		OnSucceeded: func(_ metadata.CreateTableResult) error {
			return nil
		},
		OnFailed: func(err error) error {
			panic(fmt.Sprintf("create table failed, err:%v", err))
		},
	})
	re.NoError(err)
	err = p.Start(context.Background())
	re.NoError(err)
}

func testDropTable(t *testing.T, dispatch eventdispatch.Dispatch, c *cluster.Cluster, nodeName, tableName string) {
	re := require.New(t)
	// New DropTableProcedure to drop table.
	procedure, ok, err := droptable.NewDropTableProcedure(droptable.ProcedureParams{
		ID:              0,
		Dispatch:        dispatch,
		ClusterMetadata: c.GetMetadata(),
		ClusterSnapshot: c.GetMetadata().GetClusterSnapshot(),
		SourceReq: &metaservicepb.DropTableRequest{
			Header: &metaservicepb.RequestHeader{
				Node:        nodeName,
				ClusterName: test.ClusterName,
			},
			SchemaName: test.TestSchemaName,
			Name:       tableName,
		},
		OnSucceeded: func(_ metadata.TableInfo) error {
			return nil
		},
		OnFailed: func(_ error) error {
			return nil
		},
	})
	re.NoError(err)
	re.True(ok)
	err = procedure.Start(context.Background())
	re.NoError(err)
}
