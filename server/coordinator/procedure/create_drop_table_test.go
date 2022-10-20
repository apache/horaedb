// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/stretchr/testify/require"
)

func TestCreateAndDropTable(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := MockDispatch{}
	cluster := prepare(t)
	// New CreateTableProcedure to create a new table.
	procedure := NewCreateTableProcedure(dispatch, cluster, uint64(1), &metaservicepb.CreateTableRequest{
		Header: &metaservicepb.RequestHeader{
			Node:        nodeName0,
			ClusterName: clusterName,
		},
		SchemaName: testSchemaName,
		Name:       testTableName,
		CreateSql:  "",
	}, nil, nil)
	err := procedure.Start(ctx)
	re.NoError(err)
	table, b, err := cluster.GetTable(ctx, testSchemaName, testTableName)
	re.NoError(err)
	re.Equal(b, true)
	re.NotNil(table)

	// New DropTableProcedure to drop table.
	procedure = NewDropTableProcedure(dispatch, cluster, uint64(1), &metaservicepb.DropTableRequest{
		Header: &metaservicepb.RequestHeader{
			Node:        nodeName0,
			ClusterName: clusterName,
		},
		SchemaName: table.GetSchemaName(),
		Name:       table.GetName(),
		Id:         table.GetID(),
	})
	err = procedure.Start(ctx)
	re.NoError(err)
	table, b, err = cluster.GetTable(ctx, testSchemaName, testTableName)
	re.NoError(err)
	re.Equal(b, false)
	re.Nil(table)
}
