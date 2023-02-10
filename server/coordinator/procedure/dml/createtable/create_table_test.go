// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package createtable

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/operation/scatter"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateTable(t *testing.T) {
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

	// New CreateTableProcedure to create a new table.
	p := NewProcedure(ProcedureRequest{
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
			Name:       test.TestTableName0,
		},
		OnSucceeded: func(_ cluster.CreateTableResult) error {
			return nil
		},
		OnFailed: func(_ error) error {
			return nil
		},
	})
	err = p.Start(context.Background())
	re.NoError(err)
}
