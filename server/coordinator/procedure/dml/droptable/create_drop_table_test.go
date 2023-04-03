// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package droptable

import (
	"context"
	"fmt"
	"testing"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/dml/createtable"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/operation/scatter"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateAndDropTable(t *testing.T) {
	re := require.New(t)
	dispatch := test.MockDispatch{}
	_, c := scatter.Prepare(t)

	// Select a shard in NodeName0 to open table.
	nodeShardsResult, err := c.GetNodeShards(context.Background())
	re.NoError(err)
	var shardID storage.ShardID
	var found bool
	for _, nodeShard := range nodeShardsResult.NodeShards {
		if nodeShard.ShardNode.NodeName == test.NodeName0 {
			shardID = nodeShard.ShardNode.ID
			found = true
		}
	}
	re.Equal(found, true)
	testTableNum := 20
	// Create table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", test.TestTableName0, i)
		testCreateTable(t, dispatch, c, shardID, tableName)
	}
	// Check get table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", test.TestTableName0, i)
		table, b, err := c.GetTable(test.TestSchemaName, tableName)
		re.NoError(err)
		re.Equal(b, true)
		re.NotNil(table)
	}

	// Check tables by node.
	var shardIDs []storage.ShardID
	for i := 0; i < test.DefaultShardTotal; i++ {
		shardIDs = append(shardIDs, storage.ShardID(i))
	}
	shardTables := c.GetShardTables(shardIDs)
	tableTotal := 0
	for _, v := range shardTables {
		tableTotal += len(v.Tables)
	}
	re.Equal(testTableNum, tableTotal)

	// Drop table.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", test.TestTableName0, i)
		testDropTable(t, dispatch, c, tableName)
	}
	// Check table not exists.
	for i := 0; i < testTableNum; i++ {
		tableName := fmt.Sprintf("%s_%d", test.TestTableName0, i)
		_, b, err := c.GetTable(test.TestSchemaName, tableName)
		re.NoError(err)
		re.Equal(b, false)
	}

	// Check tables by node.
	shardTables = c.GetShardTables(shardIDs)
	tableTotal = 0
	for _, v := range shardTables {
		tableTotal += len(v.Tables)
	}
	re.Equal(tableTotal, 0)
}

func testCreateTable(t *testing.T, dispatch eventdispatch.Dispatch, c *cluster.Cluster, shardID storage.ShardID, tableName string) {
	re := require.New(t)
	// New CreateTableProcedure to create a new table.
	p := createtable.NewProcedure(createtable.ProcedureRequest{
		Dispatch: dispatch,
		Cluster:  c,
		ID:       uint64(1),
		ShardID:  shardID,
		Req: &metaservicepb.CreateTableRequest{
			Header: &metaservicepb.RequestHeader{
				Node:        test.NodeName0,
				ClusterName: test.ClusterName,
			},
			SchemaName: test.TestSchemaName,
			Name:       tableName,
		},
		OnSucceeded: func(_ cluster.CreateTableResult) error {
			return nil
		},
		OnFailed: func(_ error) error {
			return nil
		},
	})
	err := p.Start(context.Background())
	re.NoError(err)
}

func testDropTable(t *testing.T, dispatch eventdispatch.Dispatch, c *cluster.Cluster, tableName string) {
	re := require.New(t)
	// New DropTableProcedure to drop table.
	procedure := NewDropTableProcedure(dispatch, c, uint64(1), &metaservicepb.DropTableRequest{
		Header: &metaservicepb.RequestHeader{
			Node:        test.NodeName0,
			ClusterName: test.ClusterName,
		},
		SchemaName: test.TestSchemaName,
		Name:       tableName,
	}, func(_ cluster.TableInfo) error {
		return nil
	}, func(_ error) error {
		return nil
	})
	err := procedure.Start(context.Background())
	re.NoError(err)
}
