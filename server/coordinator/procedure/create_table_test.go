// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/stretchr/testify/require"
)

func TestCreateTable(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := MockDispatch{}
	cluster := prepare(t)
	procedure := NewCreateTableProcedure(dispatch, cluster, uint64(1), &metaservicepb.CreateTableRequest{
		Header: &metaservicepb.RequestHeader{
			Node:        nodeName0,
			ClusterName: clusterName,
		},
		SchemaName: testSchemaName,
		Name:       testTableName,
		CreateSql:  "",
	})
	err := procedure.Start(ctx)
	re.NoError(err)
	table, b, err := cluster.GetTable(ctx, testSchemaName, testTableName)
	re.NoError(err)
	re.Equal(b, true)
	re.NotNil(table)
}
