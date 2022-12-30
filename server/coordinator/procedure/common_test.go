// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	testTableName0                         = "table0"
	testTableName1                         = "table1"
	testSchemaName                         = "testSchemaName"
	nodeName0                              = "node0"
	nodeName1                              = "node1"
	testRootPath                           = "/rootPath"
	defaultIDAllocatorStep                 = 20
	clusterName                            = "ceresdbCluster1"
	defaultNodeCount                       = 2
	defaultReplicationFactor               = 1
	defaultPartitionTableProportionOfNodes = 0.5
	defaultShardTotal                      = 4
)

type MockDispatch struct{}

func (m MockDispatch) OpenShard(_ context.Context, _ string, _ eventdispatch.OpenShardRequest) error {
	return nil
}

func (m MockDispatch) CloseShard(_ context.Context, _ string, _ eventdispatch.CloseShardRequest) error {
	return nil
}

func (m MockDispatch) CreateTableOnShard(_ context.Context, _ string, _ eventdispatch.CreateTableOnShardRequest) error {
	return nil
}

func (m MockDispatch) DropTableOnShard(_ context.Context, _ string, _ eventdispatch.DropTableOnShardRequest) error {
	return nil
}

func newTestEtcdStorage(t *testing.T) (storage.Storage, clientv3.KV, etcdutil.CloseFn) {
	_, client, closeSrv := etcdutil.PrepareEtcdServerAndClient(t)
	storage := storage.NewStorageWithEtcdBackend(client, testRootPath, storage.Options{
		MaxScanLimit: 100, MinScanLimit: 10,
	})
	return storage, client, closeSrv
}

func newTestCluster(ctx context.Context, t *testing.T) (cluster.Manager, *cluster.Cluster) {
	re := require.New(t)
	storage, kv, _ := newTestEtcdStorage(t)
	manager, err := cluster.NewManagerImpl(storage, kv, testRootPath, defaultIDAllocatorStep)
	re.NoError(err)

	cluster, err := manager.CreateCluster(ctx, clusterName, cluster.CreateClusterOpts{
		NodeCount:         defaultNodeCount,
		ReplicationFactor: defaultReplicationFactor,
		ShardTotal:        defaultShardTotal,
	})
	re.NoError(err)
	return manager, cluster
}

// Prepare a test cluster which has scattered shards and created test schema.
// Notice: sleep(5s) will be called in this function.
func prepare(t *testing.T) (cluster.Manager, *cluster.Cluster) {
	re := require.New(t)
	manager, cluster := newClusterAndRegisterNode(t)
	// Wait for the cluster to be ready.
	time.Sleep(time.Second * 5)
	_, _, err := cluster.GetOrCreateSchema(context.Background(), testSchemaName)
	re.NoError(err)
	return manager, cluster
}
