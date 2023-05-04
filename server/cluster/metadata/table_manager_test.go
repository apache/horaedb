// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// nolint
package metadata_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/CeresDB/ceresdbproto/golang/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultTimeout                  = time.Second * 10
	cluster1                        = "ceresdbCluster1"
	cluster2                        = "ceresdbCluster2"
	defaultSchema                   = "ceresdbSchema"
	defaultNodeCount                = 2
	defaultReplicationFactor        = 1
	defaultShardTotal               = 8
	defaultEnableSchedule           = true
	node1                           = "127.0.0.1:8081"
	node2                           = "127.0.0.2:8081"
	table1                          = "table1"
	table2                          = "table2"
	table3                          = "table3"
	table4                          = "table4"
	defaultSchemaID          uint32 = 0
	tableID1                 uint64 = 0
	tableID2                 uint64 = 1
	tableID3                 uint64 = 2
	tableID4                 uint64 = 3
	testRootPath                    = "/rootPath"
	defaultIDAllocatorStep          = 20
	defaultThreadNum                = 20
)

func newTestStorage(t *testing.T) (storage.Storage, clientv3.KV, *clientv3.Client, etcdutil.CloseFn) {
	_, client, closeSrv := etcdutil.PrepareEtcdServerAndClient(t)
	storage := storage.NewStorageWithEtcdBackend(client, testRootPath, storage.Options{
		MaxScanLimit: 100, MinScanLimit: 10,
	})
	return storage, client, client, closeSrv
}

func newClusterManagerWithStorage(storage storage.Storage, kv clientv3.KV, client *clientv3.Client) (cluster.Manager, error) {
	return cluster.NewManagerImpl(storage, kv, client, testRootPath, defaultIDAllocatorStep, defaultEnableSchedule)
}

func newTestClusterManager(t *testing.T) (cluster.Manager, etcdutil.CloseFn) {
	re := require.New(t)
	storage, kv, client, closeSrv := newTestStorage(t)
	manager, err := newClusterManagerWithStorage(storage, kv, client)
	re.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	err = manager.Start(ctx)
	re.NoError(err)

	return manager, closeSrv
}

func TestManagerSingleThread(t *testing.T) {
	t.Skip("Current this test is broken because of changes about cluster initialization")

	re := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	s, kv, client, closeSrv := newTestStorage(t)
	defer closeSrv()
	manager, err := newClusterManagerWithStorage(s, kv, client)
	re.NoError(err)

	re.NoError(manager.Start(ctx))

	testCreateCluster(ctx, re, manager, cluster1)

	testRegisterNode(ctx, re, manager, cluster1, node1)
	testRegisterNode(ctx, re, manager, cluster1, node2)

	testGetTables(re, manager, node1, cluster1, 0)

	testAllocSchemaID(ctx, re, manager, cluster1, defaultSchema, defaultSchemaID)
	testAllocSchemaID(ctx, re, manager, cluster1, defaultSchema, defaultSchemaID)

	for i := uint64(0); i < 5; i++ {
		testCreateTable(ctx, re, manager, node1, cluster1, defaultSchema, storage.ShardID(i), tableID1)
	}

	testRouteTables(ctx, re, manager, cluster1, defaultSchema, []string{table1, table2, table3, table4})

	testDropTable(ctx, re, manager, cluster1, defaultSchema, table1)
	testDropTable(ctx, re, manager, cluster1, defaultSchema, table3)

	testGetTables(re, manager, node1, cluster1, 1)
	testGetTables(re, manager, node2, cluster1, 1)
	testGetNodes(ctx, re, manager, cluster1)

	re.NoError(manager.Stop(ctx))

	manager, err = newClusterManagerWithStorage(s, kv, client)
	re.NoError(err)

	re.NoError(manager.Start(ctx))

	testGetTables(re, manager, node1, cluster1, 1)
	testGetTables(re, manager, node2, cluster1, 1)

	re.NoError(manager.Stop(ctx))
}

func TestManagerMultiThread(t *testing.T) {
	t.Skip("Current this test is broken because of changes about cluster initialization")

	wg := sync.WaitGroup{}
	re := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	manager, closeMgr := newTestClusterManager(t)
	defer closeMgr()
	defer re.NoError(manager.Stop(ctx))

	wg.Add(1)
	go func() {
		defer wg.Done()
		testCluster(ctx, re, manager, cluster1)
	}()
	testCluster(ctx, re, manager, cluster2)

	wg.Wait()
}

func testCluster(ctx context.Context, re *require.Assertions, manager cluster.Manager, clusterName string) {
	testCreateCluster(ctx, re, manager, clusterName)

	testRegisterNode(ctx, re, manager, clusterName, node1)
	testRegisterNode(ctx, re, manager, clusterName, node2)

	testAllocSchemaIDMultiThread(ctx, re, manager, clusterName, defaultSchema, defaultSchemaID)

	testAllocTableIDMultiThread(ctx, re, manager, clusterName, tableID1)

	testDropTable(ctx, re, manager, clusterName, defaultSchema, table1)

	testAllocTableIDMultiThread(ctx, re, manager, clusterName, tableID2)
}

func testCreateCluster(ctx context.Context, re *require.Assertions, manager cluster.Manager, clusterName string) {
	_, err := manager.CreateCluster(ctx, clusterName, metadata.CreateClusterOpts{
		NodeCount:         defaultNodeCount,
		ReplicationFactor: defaultReplicationFactor,
		ShardTotal:        defaultShardTotal,
	})
	re.NoError(err)
}

func testRegisterNode(ctx context.Context, re *require.Assertions, manager cluster.Manager,
	clusterName, nodeName string,
) {
	err := manager.RegisterNode(ctx, clusterName, metadata.RegisteredNode{
		storage.Node{
			Name:          nodeName,
			LastTouchTime: uint64(time.Now().UnixMilli()),
			State:         storage.NodeStateOnline,
		}, []metadata.ShardInfo{},
	})
	re.NoError(err)
}

func testAllocSchemaID(ctx context.Context, re *require.Assertions, manager cluster.Manager,
	cluster, schema string, schemaID uint32,
) {
	id, _, err := manager.AllocSchemaID(ctx, cluster, schema)
	re.NoError(err)
	re.Equal(schemaID, id)
}

func testCreateTable(ctx context.Context, re *require.Assertions, manager cluster.Manager,
	clusterName, schema, tableName string, shardID storage.ShardID, tableID uint64,
) {
	c, err := manager.GetCluster(ctx, clusterName)
	re.NoError(err)
	table, err := c.GetMetadata().CreateTable(ctx, metadata.CreateTableRequest{
		ShardID:       shardID,
		SchemaName:    schema,
		TableName:     tableName,
		PartitionInfo: storage.PartitionInfo{},
	})
	re.NoError(err)
	re.Equal(tableID, table.Table.ID)
}

func testGetTables(re *require.Assertions, manager cluster.Manager, node, cluster string, num int) {
	shardIDs := make([]storage.ShardID, 0, defaultShardTotal)
	for i := 0; i < defaultShardTotal; i++ {
		shardIDs = append(shardIDs, storage.ShardID(i))
	}
	shardTables, err := manager.GetTables(cluster, node, shardIDs)
	re.NoError(err)
	re.Equal(4, len(shardTables))

	tableNum := 0
	for _, tables := range shardTables {
		re.Equal(clusterpb.ShardRole_LEADER, tables.Shard.Role)
		tableNum += len(tables.Tables)
	}
	re.Equal(num, tableNum)
}

func testRouteTables(ctx context.Context, re *require.Assertions, manager cluster.Manager, cluster, schema string, tableNames []string) {
	ret, err := manager.RouteTables(ctx, cluster, schema, tableNames)
	re.NoError(err)
	re.Equal(uint64(0), ret.ClusterViewVersion)
	re.Equal(len(tableNames), len(ret.RouteEntries))
	for _, entry := range ret.RouteEntries {
		re.Equal(1, len(entry.NodeShards))
		re.Equal(storage.ShardRoleLeader, entry.NodeShards[0].ShardNode.ShardRole)
	}
}

func testDropTable(ctx context.Context, re *require.Assertions, manager cluster.Manager, clusterName string, schemaName string, tableName string) {
	err := manager.DropTable(ctx, clusterName, schemaName, tableName)
	re.NoError(err)
}

func testAllocSchemaIDMultiThread(ctx context.Context, re *require.Assertions, manager cluster.Manager, clusterName string, schemaName string, schemaID uint32) {
	wg := sync.WaitGroup{}
	for i := 0; i < defaultThreadNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			testAllocSchemaID(ctx, re, manager, clusterName, schemaName, schemaID)
		}()
	}

	wg.Wait()
}

func testAllocTableIDMultiThread(ctx context.Context, re *require.Assertions, manager cluster.Manager, clusterName string, tableID uint64) {
	wg := sync.WaitGroup{}
	for i := 0; i < defaultThreadNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			testCreateTable(ctx, re, manager, clusterName, defaultSchema, table1, 0, tableID)
		}()
	}

	testCreateTable(ctx, re, manager, clusterName, defaultSchema, table1, 1, tableID)
	wg.Wait()
}

func testGetNodes(ctx context.Context, re *require.Assertions, manager cluster.Manager, cluster string) {
	getNodesResult, err := manager.GetNodeShards(ctx, cluster)
	re.NoError(err)
	re.Equal(defaultShardTotal, len(getNodesResult.NodeShards))
}
