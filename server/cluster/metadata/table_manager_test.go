// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

package metadata_test

import (
	"context"
	"path"
	"testing"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	TestRootPath        = "/testRootPath"
	TestClusterID       = 0
	TestClusterName     = "TestClusterName"
	TestSchemaIDPrefix  = "TestSchemaIDPrefix"
	TestTableIDPrefix   = "TestTableIDPrefix"
	TestIDAllocatorStep = 5
	TestSchemaName      = "TestSchemaName"
	TestTableName       = "TestTableName"
)

func TestTableManager(t *testing.T) {
	ctx := context.Background()
	re := require.New(t)

	_, client, _ := etcdutil.PrepareEtcdServerAndClient(t)
	clusterStorage := storage.NewStorageWithEtcdBackend(client, TestRootPath, storage.Options{
		MaxScanLimit: 100, MinScanLimit: 10, MaxOpsPerTxn: 10,
	})

	schemaIDAlloc := id.NewAllocatorImpl(zap.NewNop(), client, path.Join(TestRootPath, TestClusterName, TestSchemaIDPrefix), TestIDAllocatorStep)
	tableIDAlloc := id.NewAllocatorImpl(zap.NewNop(), client, path.Join(TestRootPath, TestClusterName, TestTableIDPrefix), TestIDAllocatorStep)
	tableManager := metadata.NewTableManagerImpl(zap.NewNop(), clusterStorage, storage.ClusterID(TestClusterID), schemaIDAlloc, tableIDAlloc)
	err := tableManager.Load(ctx)
	re.NoError(err)

	testSchema(ctx, re, tableManager)
	testCreateAndDropTable(ctx, re, tableManager)
}

func testSchema(ctx context.Context, re *require.Assertions, manager metadata.TableManager) {
	_, exists := manager.GetSchema(TestSchemaName)
	re.False(exists)
	schema, exists, err := manager.GetOrCreateSchema(ctx, TestSchemaName)
	re.NoError(err)
	re.False(exists)
	re.Equal(TestSchemaName, schema.Name)
}

func testCreateAndDropTable(ctx context.Context, re *require.Assertions, manager metadata.TableManager) {
	_, exists, err := manager.GetTable(TestSchemaName, TestTableName)
	re.NoError(err)
	re.False(exists)

	t, err := manager.CreateTable(ctx, TestSchemaName, TestTableName, storage.PartitionInfo{})
	re.NoError(err)
	re.Equal(TestTableName, t.Name)

	t, exists, err = manager.GetTable(TestSchemaName, TestTableName)
	re.NoError(err)
	re.True(exists)
	re.Equal(TestTableName, t.Name)

	err = manager.DropTable(ctx, TestSchemaName, TestTableName)
	re.NoError(err)

	_, exists, err = manager.GetTable(TestSchemaName, TestTableName)
	re.NoError(err)
	re.False(exists)
}
