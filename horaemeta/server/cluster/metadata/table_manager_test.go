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

package metadata_test

import (
	"context"
	"path"
	"testing"

	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/etcdutil"
	"github.com/apache/incubator-horaedb-meta/server/id"
	"github.com/apache/incubator-horaedb-meta/server/storage"
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

	t, err := manager.CreateTable(ctx, TestSchemaName, TestTableName, storage.PartitionInfo{Info: nil})
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
