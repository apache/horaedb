// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/schedule"
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
	defaultLease                    = 100
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

func newTestStorage(t *testing.T) (storage.Storage, clientv3.KV, etcdutil.CloseFn) {
	_, client, closeSrv := etcdutil.PrepareEtcdServerAndClient(t)
	storage := storage.NewStorageWithEtcdBackend(client, testRootPath, storage.Options{
		MaxScanLimit: 100, MinScanLimit: 10,
	})
	return storage, client, closeSrv
}

func newClusterManagerWithStorage(storage storage.Storage, kv clientv3.KV) (Manager, error) {
	return NewManagerImpl(storage, kv, schedule.NewHeartbeatStreams(context.Background()), testRootPath, defaultIDAllocatorStep)
}

func newTestClusterManager(t *testing.T) (Manager, etcdutil.CloseFn) {
	re := require.New(t)
	storage, kv, closeSrv := newTestStorage(t)
	manager, err := newClusterManagerWithStorage(storage, kv)
	re.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	err = manager.Start(ctx)
	re.NoError(err)

	return manager, closeSrv
}

func TestManagerSingleThread(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	storage, kv, closeSrv := newTestStorage(t)
	defer closeSrv()
	manager, err := newClusterManagerWithStorage(storage, kv)
	re.NoError(err)

	re.NoError(manager.Start(ctx))

	testCreateCluster(ctx, re, manager, cluster1)

	testRegisterNode(ctx, re, manager, cluster1, node1, defaultLease)
	testRegisterNode(ctx, re, manager, cluster1, node2, defaultLease)

	testGetTables(ctx, re, manager, node1, cluster1, 0)

	testAllocSchemaID(ctx, re, manager, cluster1, defaultSchema, defaultSchemaID)
	testAllocSchemaID(ctx, re, manager, cluster1, defaultSchema, defaultSchemaID)

	testAllocTableID(ctx, re, manager, node1, cluster1, defaultSchema, table1, tableID1)
	testAllocTableID(ctx, re, manager, node1, cluster1, defaultSchema, table1, tableID1)
	testAllocTableID(ctx, re, manager, node1, cluster1, defaultSchema, table2, tableID2)
	testAllocTableID(ctx, re, manager, node2, cluster1, defaultSchema, table3, tableID3)
	testAllocTableID(ctx, re, manager, node2, cluster1, defaultSchema, table4, tableID4)

	testRouteTables(ctx, re, manager, cluster1, defaultSchema, []string{table1, table2, table3, table4})

	testDropTable(ctx, re, manager, cluster1, defaultSchema, table1, tableID1)
	testDropTable(ctx, re, manager, cluster1, defaultSchema, table3, tableID3)

	testGetTables(ctx, re, manager, node1, cluster1, 1)
	testGetTables(ctx, re, manager, node2, cluster1, 1)
	testGetNodes(ctx, re, manager, cluster1)

	re.NoError(manager.Stop(ctx))

	manager, err = newClusterManagerWithStorage(storage, kv)
	re.NoError(err)

	re.NoError(manager.Start(ctx))

	testGetTables(ctx, re, manager, node1, cluster1, 1)
	testGetTables(ctx, re, manager, node2, cluster1, 1)

	re.NoError(manager.Stop(ctx))
}

func TestManagerMultiThread(t *testing.T) {
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

func testCluster(ctx context.Context, re *require.Assertions, manager Manager, clusterName string) {
	testCreateCluster(ctx, re, manager, clusterName)

	testRegisterNode(ctx, re, manager, clusterName, node1, defaultLease)
	testRegisterNode(ctx, re, manager, clusterName, node2, defaultLease)

	testAllocSchemaIDMultiThread(ctx, re, manager, clusterName, defaultSchema, defaultSchemaID)

	testAllocTableIDMultiThread(ctx, re, manager, clusterName, tableID1)

	testDropTable(ctx, re, manager, clusterName, defaultSchema, table1, tableID1)

	testAllocTableIDMultiThread(ctx, re, manager, clusterName, tableID2)
}

func testCreateCluster(ctx context.Context, re *require.Assertions, manager Manager, clusterName string) {
	_, err := manager.CreateCluster(ctx, clusterName, defaultNodeCount, defaultReplicationFactor, defaultShardTotal)
	re.NoError(err)
}

func testRegisterNode(ctx context.Context, re *require.Assertions, manager Manager,
	cluster, node string, lease uint32,
) {
	err := manager.RegisterNode(ctx, cluster, &metaservicepb.NodeInfo{
		Endpoint: node,
		Lease:    lease,
	})
	re.NoError(err)
}

func testAllocSchemaID(ctx context.Context, re *require.Assertions, manager Manager,
	cluster, schema string, schemaID uint32,
) {
	id, err := manager.AllocSchemaID(ctx, cluster, schema)
	re.NoError(err)
	re.Equal(schemaID, id)
}

func testAllocTableID(ctx context.Context, re *require.Assertions, manager Manager,
	node, cluster, schema, tableName string, tableID uint64,
) {
	table, err := manager.AllocTableID(ctx, cluster, schema, tableName, node)
	re.NoError(err)
	re.Equal(tableID, table.GetID())
}

func testGetTables(ctx context.Context, re *require.Assertions, manager Manager, node, cluster string, num int) {
	shardIDs, err := manager.GetShards(ctx, cluster, node)
	re.NoError(err)

	shardTables, err := manager.GetTables(ctx, cluster, node, shardIDs)
	re.NoError(err)
	re.Equal(4, len(shardTables))

	tableNum := 0
	for _, tables := range shardTables {
		re.Equal(clusterpb.ShardRole_LEADER, tables.ShardRole)
		tableNum += len(tables.Tables)
	}
	re.Equal(num, tableNum)
}

func testRouteTables(ctx context.Context, re *require.Assertions, manager Manager, cluster, schema string, tableNames []string) {
	ret, err := manager.RouteTables(ctx, cluster, schema, tableNames)
	re.NoError(err)
	re.Equal(uint64(0), ret.Version)
	re.Equal(len(tableNames), len(ret.RouteEntries))
	for _, entry := range ret.RouteEntries {
		re.Equal(1, len(entry.NodeShards))
		re.Equal(clusterpb.ShardRole_LEADER, entry.NodeShards[0].ShardInfo.ShardRole)
	}
}

func testDropTable(ctx context.Context, re *require.Assertions, manager Manager, clusterName string, schemaName string, tableName string, tableID uint64) {
	err := manager.DropTable(ctx, clusterName, schemaName, tableName, tableID)
	re.NoError(err)
}

func testAllocSchemaIDMultiThread(ctx context.Context, re *require.Assertions, manager Manager, clusterName string, schemaName string, schemaID uint32) {
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

func testAllocTableIDMultiThread(ctx context.Context, re *require.Assertions, manager Manager, clusterName string, tableID uint64) {
	wg := sync.WaitGroup{}
	for i := 0; i < defaultThreadNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			testAllocTableID(ctx, re, manager, node1, clusterName, defaultSchema, table1, tableID)
		}()
	}

	testAllocTableID(ctx, re, manager, node2, clusterName, defaultSchema, table1, tableID)
	wg.Wait()
}

func testGetNodes(ctx context.Context, re *require.Assertions, manager Manager, cluster string) {
	getNodesResult, err := manager.GetNodes(ctx, cluster)
	re.NoError(err)
	re.Equal(defaultShardTotal, len(getNodesResult.NodeShards))
}
