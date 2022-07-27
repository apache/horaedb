// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const (
	defaultCluster                  = "ceresdbCluster"
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
)

func prepareEtcdServerAndClient(t *testing.T) (*embed.Etcd, *clientv3.Client, func()) {
	cfg := etcdutil.NewTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)

	<-etcd.Server.ReadyNotify()

	endpoint := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint},
	})
	assert.NoError(t, err)

	clean := func() {
		etcd.Close()
		etcdutil.CleanConfig(cfg)
	}
	return etcd, client, clean
}

func newClusterManager(t *testing.T) Manager {
	_, client, _ := prepareEtcdServerAndClient(t)
	storage := storage.NewStorageWithEtcdBackend(client, "/", storage.Options{
		MaxScanLimit: 100, MinScanLimit: 10,
	})
	return NewManagerImpl(storage)
}

func TestManager(t *testing.T) {
	re := require.New(t)
	manager := newClusterManager(t)

	ctx := context.Background()
	testCreateCluster(ctx, re, manager)

	testRegisterNode(ctx, re, manager, defaultCluster, node1, defaultLease)
	testRegisterNode(ctx, re, manager, defaultCluster, node2, defaultLease)

	testAllocSchemaID(ctx, re, manager, defaultCluster, defaultSchema, defaultSchemaID)

	testAllocTableID(ctx, re, manager, node1, defaultCluster, defaultSchema, table1, tableID1)
	testAllocTableID(ctx, re, manager, node1, defaultCluster, defaultSchema, table1, tableID1)
	testAllocTableID(ctx, re, manager, node1, defaultCluster, defaultSchema, table2, tableID2)
	testAllocTableID(ctx, re, manager, node2, defaultCluster, defaultSchema, table3, tableID3)
	testAllocTableID(ctx, re, manager, node2, defaultCluster, defaultSchema, table4, tableID4)

	testGetTables(ctx, re, manager, node1, defaultCluster)
	testGetTables(ctx, re, manager, node2, defaultCluster)
}

func testCreateCluster(ctx context.Context, re *require.Assertions, manager Manager) {
	_, err := manager.CreateCluster(ctx, defaultCluster, defaultNodeCount, defaultReplicationFactor, defaultShardTotal)
	re.NoError(err)
}

func testRegisterNode(ctx context.Context, re *require.Assertions, manager Manager,
	cluster, node string, lease uint32,
) {
	err := manager.RegisterNode(ctx, cluster, node, lease)
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
	node, cluster, schema, table string, tableID uint64,
) {
	id, err := manager.AllocTableID(ctx, node, cluster, schema, table)
	re.NoError(err)
	re.Equal(tableID, id)
}

func testGetTables(ctx context.Context, re *require.Assertions, manager Manager, node, cluster string) {
	shardIDs, err := manager.GetShards(ctx, cluster, node)
	re.NoError(err)

	shardTables, err := manager.GetTables(ctx, node, cluster, shardIDs)
	re.NoError(err)

	tableNum := 0
	for _, tables := range shardTables {
		re.Equal(clusterpb.ShardRole_LEADER, tables.shardRole)
		tableNum += len(tables.tables)
	}
	re.Equal(2, tableNum)
}
