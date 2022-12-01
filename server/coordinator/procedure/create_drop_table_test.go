// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"fmt"
	"testing"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateAndDropTable(t *testing.T) {
	re := require.New(t)
	dispatch := MockDispatch{}
	c := prepare(t)
	testTableNum := 20
	// Create table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", testTableName0, i)
		testCreateTable(t, dispatch, c, tableName)
	}
	// Check get table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", testTableName0, i)
		table, b, err := c.GetTable(testSchemaName, tableName)
		re.NoError(err)
		re.Equal(b, true)
		re.NotNil(table)
	}

	// Check tables by node.
	var shardIDs []storage.ShardID
	for i := 0; i < defaultShardTotal; i++ {
		shardIDs = append(shardIDs, storage.ShardID(i))
	}
	shardTables := c.GetShardTables(shardIDs, nodeName0)
	tableTotal := 0
	for _, v := range shardTables {
		tableTotal += len(v.Tables)
	}
	re.Equal(testTableNum, tableTotal)

	// Drop table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", testTableName0, i)
		testDropTable(t, dispatch, c, tableName)
	}
	// Check table not exists.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", testTableName0, i)
		_, b, err := c.GetTable(testSchemaName, tableName)
		re.NoError(err)
		re.Equal(b, false)
	}

	// Check tables by node.
	shardTables = c.GetShardTables(shardIDs, nodeName0)
	tableTotal = 0
	for _, v := range shardTables {
		tableTotal += len(v.Tables)
	}
	re.Equal(tableTotal, 0)
}

func testCreateTable(t *testing.T, dispatch eventdispatch.Dispatch, c *cluster.Cluster, tableName string) {
	re := require.New(t)
	// New CreateTableProcedure to create a new table.
	procedure := NewCreateTableProcedure(dispatch, c, uint64(1), &metaservicepb.CreateTableRequest{
		Header: &metaservicepb.RequestHeader{
			Node:        nodeName0,
			ClusterName: clusterName,
		},
		SchemaName: testSchemaName,
		Name:       tableName,
	}, func(_ cluster.CreateTableResult) error {
		return nil
	}, func(_ error) error {
		return nil
	})
	err := procedure.Start(context.Background())
	re.NoError(err)
}

func testDropTable(t *testing.T, dispatch eventdispatch.Dispatch, c *cluster.Cluster, tableName string) {
	re := require.New(t)
	// New DropTableProcedure to drop table.
	procedure := NewDropTableProcedure(dispatch, c, uint64(1), &metaservicepb.DropTableRequest{
		Header: &metaservicepb.RequestHeader{
			Node:        nodeName0,
			ClusterName: clusterName,
		},
		SchemaName: testSchemaName,
		Name:       tableName,
	}, func(_ cluster.TableInfo) error {
		return nil
	}, func(_ error) error {
		return nil
	})
	err := procedure.Start(context.Background())
	re.NoError(err)
}
