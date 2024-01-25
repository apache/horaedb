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

package createtable_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/ddl/createtable"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/test"
	"github.com/apache/incubator-horaedb-proto/golang/pkg/metaservicepb"
	"github.com/stretchr/testify/require"
)

func TestCreateTable(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := test.MockDispatch{}
	c := test.InitStableCluster(ctx, t)

	// Select a shard to create table.
	snapshot := c.GetMetadata().GetClusterSnapshot()
	shardNode := snapshot.Topology.ClusterView.ShardNodes[0]

	// New CreateTableProcedure to create a new table.
	p, err := createtable.NewProcedure(createtable.ProcedureParams{
		Dispatch:        dispatch,
		ClusterMetadata: c.GetMetadata(),
		ClusterSnapshot: snapshot,
		ID:              uint64(1),
		ShardID:         shardNode.ID,
		SourceReq: &metaservicepb.CreateTableRequest{
			Header: &metaservicepb.RequestHeader{
				Node:        shardNode.NodeName,
				ClusterName: test.ClusterName,
			},
			SchemaName: test.TestSchemaName,
			Name:       test.TestTableName0,
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
