// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"path"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	AllocClusterIDPrefix = "ClusterID"
	AllocSchemaIDPrefix  = "SchemaID"
	AllocTableIDPrefix   = "TableID"
)

type Manager interface {
	// Start must be called before manager is used.
	Start(ctx context.Context) error
	// Stop must be called before manager is dropped.
	Stop(ctx context.Context) error

	ListClusters(ctx context.Context) ([]*Cluster, error)
	CreateCluster(ctx context.Context, clusterName string, opts CreateClusterOpts) (*Cluster, error)
	GetCluster(ctx context.Context, clusterName string) (*Cluster, error)
	// AllocSchemaID means get or create schema.
	// The second output parameter bool: Returns true if the table was newly created.
	AllocSchemaID(ctx context.Context, clusterName, schemaName string) (storage.SchemaID, bool, error)
	GetTables(clusterName, nodeName string, shardIDs []storage.ShardID) (map[storage.ShardID]ShardTables, error)
	DropTable(ctx context.Context, clusterName, schemaName, tableName string) error
	RouteTables(ctx context.Context, clusterName, schemaName string, tableNames []string) (RouteTablesResult, error)
	GetNodeShards(ctx context.Context, clusterName string) (GetNodeShardsResult, error)

	RegisterNode(ctx context.Context, clusterName string, registeredNode RegisteredNode) error
	GetRegisteredNode(ctx context.Context, clusterName string, node string) (RegisteredNode, error)
}

type managerImpl struct {
	// RWMutex is used to protect clusters when creating new cluster.
	lock     sync.RWMutex
	running  bool
	clusters map[string]*Cluster

	storage         storage.Storage
	kv              clientv3.KV
	alloc           id.Allocator
	rootPath        string
	idAllocatorStep uint
}

func NewManagerImpl(storage storage.Storage, kv clientv3.KV, rootPath string, idAllocatorStep uint) (Manager, error) {
	alloc := id.NewAllocatorImpl(kv, path.Join(rootPath, AllocClusterIDPrefix), idAllocatorStep)

	manager := &managerImpl{
		storage:         storage,
		kv:              kv,
		alloc:           alloc,
		clusters:        make(map[string]*Cluster, 0),
		rootPath:        rootPath,
		idAllocatorStep: idAllocatorStep,
	}

	return manager, nil
}

func (m *managerImpl) ListClusters(_ context.Context) ([]*Cluster, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	clusters := make([]*Cluster, 0, len(m.clusters))
	for _, cluster := range m.clusters {
		clusters = append(clusters, cluster)
	}
	return clusters, nil
}

func (m *managerImpl) CreateCluster(ctx context.Context, clusterName string, opts CreateClusterOpts) (*Cluster, error) {
	if opts.NodeCount < 1 {
		log.Error("cluster's nodeCount must > 0", zap.String("clusterName", clusterName))
		return nil, ErrCreateCluster.WithCausef("nodeCount must > 0")
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	cluster, ok := m.clusters[clusterName]
	if ok {
		return cluster, ErrClusterAlreadyExists
	}

	clusterID, err := m.allocClusterID(ctx)
	if err != nil {
		log.Error("fail to alloc cluster id", zap.Error(err), zap.String("clusterName", clusterName))
		return nil, errors.WithMessagef(err, "cluster manager CreateCluster, clusterName:%s", clusterName)
	}

	clusterMetadata := storage.Cluster{
		ID:                clusterID,
		Name:              clusterName,
		MinNodeCount:      opts.NodeCount,
		ReplicationFactor: opts.ReplicationFactor,
		ShardTotal:        opts.ShardTotal,
		CreatedAt:         uint64(time.Now().UnixMilli()),
	}
	err = m.storage.CreateCluster(ctx, storage.CreateClusterRequest{
		Cluster: clusterMetadata,
	})
	if err != nil {
		log.Error("fail to create cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return nil, errors.WithMessage(err, "cluster create cluster")
	}

	cluster = NewCluster(clusterMetadata, m.storage, m.kv, m.rootPath, m.idAllocatorStep)

	if err = cluster.init(ctx); err != nil {
		log.Error("fail to init cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return nil, errors.WithMessage(err, "cluster init")
	}

	if err := cluster.load(ctx); err != nil {
		log.Error("fail to load cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return nil, errors.WithMessage(err, "cluster load")
	}

	m.clusters[clusterName] = cluster

	return cluster, nil
}

func (m *managerImpl) GetCluster(_ context.Context, clusterName string) (*Cluster, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	cluster, exist := m.clusters[clusterName]
	if exist {
		return cluster, nil
	}
	return nil, ErrClusterNotFound
}

func (m *managerImpl) AllocSchemaID(ctx context.Context, clusterName, schemaName string) (storage.SchemaID, bool, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return 0, false, errors.WithMessage(err, "get cluster")
	}

	// create new schema
	schema, exists, err := cluster.GetOrCreateSchema(ctx, schemaName)
	if err != nil {
		log.Error("fail to create schema", zap.Error(err))
		return 0, false, errors.WithMessage(err, "get or create schema")
	}
	return schema.ID, exists, nil
}

func (m *managerImpl) GetTables(clusterName, nodeName string, shardIDs []storage.ShardID) (map[storage.ShardID]ShardTables, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		return nil, errors.WithMessage(err, "get cluster")
	}

	shardTables := cluster.GetShardTables(shardIDs, nodeName)
	return shardTables, nil
}

func (m *managerImpl) DropTable(ctx context.Context, clusterName, schemaName, tableName string) error {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return errors.WithMessage(err, "get cluster")
	}

	_, err = cluster.DropTable(ctx, schemaName, tableName)
	if err != nil {
		return errors.WithMessage(err, "cluster drop table")
	}

	return nil
}

func (m *managerImpl) RegisterNode(ctx context.Context, clusterName string, registeredNode RegisteredNode) error {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if !m.running {
		return nil
	}

	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return errors.WithMessage(err, "get cluster")
	}
	err = cluster.RegisterNode(ctx, registeredNode)
	if err != nil {
		return errors.WithMessage(err, "cluster register node")
	}

	return nil
}

func (m *managerImpl) GetRegisteredNode(_ context.Context, clusterName string, nodeName string) (RegisteredNode, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return RegisteredNode{}, errors.WithMessage(err, "get cluster")
	}

	registeredNode, ok := cluster.GetRegisteredNodeByName(nodeName)
	if !ok {
		return RegisteredNode{}, ErrNodeNotFound.WithCausef("registeredNode is not found, registeredNode:%s, cluster:%s", nodeName, clusterName)
	}

	return registeredNode, nil
}

func (m *managerImpl) getCluster(clusterName string) (*Cluster, error) {
	m.lock.RLock()
	cluster, ok := m.clusters[clusterName]
	m.lock.RUnlock()
	if !ok {
		return nil, ErrClusterNotFound.WithCausef("cluster name:%s", clusterName)
	}
	return cluster, nil
}

func (m *managerImpl) allocClusterID(ctx context.Context) (storage.ClusterID, error) {
	ID, err := m.alloc.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessagef(err, "alloc cluster id")
	}
	return storage.ClusterID(ID), nil
}

func (m *managerImpl) Start(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.running {
		log.Warn("cluster manager has already been started")
		return nil
	}

	clusters, err := m.storage.ListClusters(ctx)
	if err != nil {
		log.Error("cluster manager fail to start, fail to list clusters", zap.Error(err))
		return errors.WithMessage(err, "cluster manager start")
	}

	m.clusters = make(map[string]*Cluster, len(clusters.Clusters))
	for _, cluster := range clusters.Clusters {
		cluster := NewCluster(cluster, m.storage, m.kv, m.rootPath, m.idAllocatorStep)
		if err := cluster.load(ctx); err != nil {
			log.Error("fail to load cluster", zap.String("cluster", cluster.Name()), zap.Error(err))
			return errors.WithMessage(err, "fail to load cluster")
		}
		log.Info("open cluster successfully", zap.String("cluster", cluster.Name()))
		m.clusters[cluster.Name()] = cluster
	}
	m.running = true

	return nil
}

func (m *managerImpl) Stop(_ context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.running {
		return nil
	}

	m.clusters = make(map[string]*Cluster)
	m.running = false
	return nil
}

func (m *managerImpl) RouteTables(ctx context.Context, clusterName, schemaName string, tableNames []string) (RouteTablesResult, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return RouteTablesResult{}, errors.WithMessage(err, "get cluster")
	}

	ret, err := cluster.RouteTables(ctx, schemaName, tableNames)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return RouteTablesResult{}, errors.WithMessage(err, "cluster route tables")
	}

	return ret, nil
}

func (m *managerImpl) GetNodeShards(ctx context.Context, clusterName string) (GetNodeShardsResult, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return GetNodeShardsResult{}, errors.WithMessage(err, "get cluster")
	}

	ret, err := cluster.GetNodeShards(ctx)
	if err != nil {
		return GetNodeShardsResult{}, errors.WithMessage(err, "cluster get NodeShards")
	}

	return ret, nil
}
