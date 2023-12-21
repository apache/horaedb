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

package droptable_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/incubator-horaedb-meta/server/cluster"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/eventdispatch"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/ddl/createtable"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/ddl/droptable"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/test"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/apache/incubator-horaedb-proto/golang/pkg/metaservicepb"
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
