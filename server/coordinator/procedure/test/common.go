// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package test

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	TestTableName0                     = "table0"
	TestTableName1                     = "table1"
	TestSchemaName                     = "TestSchemaName"
	TestRootPath                       = "/rootPath"
	DefaultIDAllocatorStep             = 20
	ClusterName                        = "ceresdbCluster1"
	DefaultNodeCount                   = 2
	DefaultShardTotal                  = 4
	DefaultSchedulerOperator           = true
	DefaultTopologyType                = "static"
	DefaultProcedureExecutingBatchSize = math.MaxUint32
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

func (m MockDispatch) CloseTableOnShard(_ context.Context, _ string, _ eventdispatch.CloseTableOnShardRequest) error {
	return nil
}

func (m MockDispatch) OpenTableOnShard(_ context.Context, _ string, _ eventdispatch.OpenTableOnShardRequest) error {
	return nil
}

type MockStorage struct{}

func (m MockStorage) CreateOrUpdate(_ context.Context, _ procedure.Meta) error {
	return nil
}

func (m MockStorage) List(_ context.Context, _ int) ([]*procedure.Meta, error) {
	return nil, nil
}

func (m MockStorage) MarkDeleted(_ context.Context, _ uint64) error {
	return nil
}

func NewTestStorage(_ *testing.T) procedure.Storage {
	return MockStorage{}
}

type MockIDAllocator struct{}

func (m MockIDAllocator) Alloc(_ context.Context) (uint64, error) {
	return 0, nil
}

func (m MockIDAllocator) Collect(_ context.Context, _ uint64) error {
	return nil
}

// InitEmptyCluster will return a cluster that has created shards and nodes, but it does not have any shard node mapping.
func InitEmptyCluster(ctx context.Context, t *testing.T) *cluster.Cluster {
	re := require.New(t)

	_, client, _ := etcdutil.PrepareEtcdServerAndClient(t)
	clusterStorage := storage.NewStorageWithEtcdBackend(client, TestRootPath, storage.Options{
		MaxScanLimit: 100, MinScanLimit: 10, MaxOpsPerTxn: 10,
	})

	logger := zap.NewNop()

	clusterMetadata := metadata.NewClusterMetadata(logger, storage.Cluster{
		ID:                          0,
		Name:                        ClusterName,
		MinNodeCount:                DefaultNodeCount,
		ShardTotal:                  DefaultShardTotal,
		EnableSchedule:              DefaultSchedulerOperator,
		TopologyType:                DefaultTopologyType,
		ProcedureExecutingBatchSize: DefaultProcedureExecutingBatchSize,
		CreatedAt:                   0,
	}, clusterStorage, client, TestRootPath, DefaultIDAllocatorStep)

	err := clusterMetadata.Init(ctx)
	re.NoError(err)

	err = clusterMetadata.Load(ctx)
	re.NoError(err)

	c, err := cluster.NewCluster(logger, clusterMetadata, client, TestRootPath)
	re.NoError(err)

	_, _, err = c.GetMetadata().GetOrCreateSchema(ctx, TestSchemaName)
	re.NoError(err)

	lastTouchTime := time.Now().UnixMilli()
	for i := 0; i < DefaultNodeCount; i++ {
		err = c.GetMetadata().RegisterNode(ctx, metadata.RegisteredNode{
			Node:       storage.Node{Name: fmt.Sprintf("node%d", i), LastTouchTime: uint64(lastTouchTime)},
			ShardInfos: nil,
		})
		re.NoError(err)
	}

	return c
}

// InitPrepareCluster will return a cluster that has created shards and nodes, and cluster state is prepare.
func InitPrepareCluster(ctx context.Context, t *testing.T) *cluster.Cluster {
	re := require.New(t)
	c := InitEmptyCluster(ctx, t)

	err := c.GetMetadata().UpdateClusterView(ctx, storage.ClusterStatePrepare, []storage.ShardNode{})
	re.NoError(err)

	return c
}

// InitStableCluster will return a cluster that has created shards and nodes, and shards have been assigned to existing nodes.
func InitStableCluster(ctx context.Context, t *testing.T) *cluster.Cluster {
	re := require.New(t)
	c := InitEmptyCluster(ctx, t)
	snapshot := c.GetMetadata().GetClusterSnapshot()
	shardNodes := make([]storage.ShardNode, 0, DefaultShardTotal)
	for _, shardView := range snapshot.Topology.ShardViewsMapping {
		selectNodeIdx, err := rand.Int(rand.Reader, big.NewInt(int64(len(snapshot.RegisteredNodes))))
		re.NoError(err)
		shardNodes = append(shardNodes, storage.ShardNode{
			ID:        shardView.ShardID,
			ShardRole: storage.ShardRoleLeader,
			NodeName:  snapshot.RegisteredNodes[selectNodeIdx.Int64()].Node.Name,
		})
	}

	err := c.GetMetadata().UpdateClusterView(ctx, storage.ClusterStateStable, shardNodes)
	re.NoError(err)

	return c
}
