// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	AllocClusterIDPrefix = "ClusterID"
)

type Manager interface {
	// Start must be called before manager is used.
	Start(ctx context.Context) error
	// Stop must be called before manager is dropped.
	Stop(ctx context.Context) error

	ListClusters(ctx context.Context) ([]*Cluster, error)
	CreateCluster(ctx context.Context, clusterName string, opts metadata.CreateClusterOpts) (*Cluster, error)
	UpdateCluster(ctx context.Context, clusterName string, opt metadata.UpdateClusterOpts) error
	GetCluster(ctx context.Context, clusterName string) (*Cluster, error)
	// AllocSchemaID means get or create schema.
	// The second output parameter bool: Returns true if the table was newly created.
	AllocSchemaID(ctx context.Context, clusterName, schemaName string) (storage.SchemaID, bool, error)
	GetTables(clusterName, nodeName string, shardIDs []storage.ShardID) (map[storage.ShardID]metadata.ShardTables, error)
	DropTable(ctx context.Context, clusterName, schemaName, tableName string) error
	RouteTables(ctx context.Context, clusterName, schemaName string, tableNames []string) (metadata.RouteTablesResult, error)
	GetNodeShards(ctx context.Context, clusterName string) (metadata.GetNodeShardsResult, error)

	RegisterNode(ctx context.Context, clusterName string, registeredNode metadata.RegisteredNode) error
	GetRegisteredNode(ctx context.Context, clusterName string, node string) (metadata.RegisteredNode, error)
	ListRegisterNodes(ctx context.Context, clusterName string) ([]metadata.RegisteredNode, error)
}

type managerImpl struct {
	// RWMutex is used to protect clusters when creating new cluster.
	lock     sync.RWMutex
	running  bool
	clusters map[string]*Cluster

	storage         storage.Storage
	kv              clientv3.KV
	client          *clientv3.Client
	alloc           id.Allocator
	rootPath        string
	idAllocatorStep uint

	// TODO: topologyType is used to be compatible with cluster data changes and needs to be deleted later.
	topologyType storage.TopologyType
}

func NewManagerImpl(storage storage.Storage, kv clientv3.KV, client *clientv3.Client, rootPath string, idAllocatorStep uint, topologyType storage.TopologyType) (Manager, error) {
	alloc := id.NewAllocatorImpl(log.GetLogger(), kv, path.Join(rootPath, AllocClusterIDPrefix), idAllocatorStep)

	manager := &managerImpl{
		storage:         storage,
		kv:              kv,
		client:          client,
		alloc:           alloc,
		clusters:        make(map[string]*Cluster, 0),
		rootPath:        rootPath,
		idAllocatorStep: idAllocatorStep,
		topologyType:    topologyType,
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

func (m *managerImpl) CreateCluster(ctx context.Context, clusterName string, opts metadata.CreateClusterOpts) (*Cluster, error) {
	if opts.NodeCount < 1 {
		log.Error("cluster's nodeCount must > 0", zap.String("clusterName", clusterName))
		return nil, metadata.ErrCreateCluster.WithCausef("nodeCount must > 0")
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	cluster, ok := m.clusters[clusterName]
	if ok {
		return cluster, metadata.ErrClusterAlreadyExists
	}

	clusterID, err := m.allocClusterID(ctx)
	if err != nil {
		log.Error("fail to alloc cluster id", zap.Error(err), zap.String("clusterName", clusterName))
		return nil, errors.WithMessagef(err, "cluster manager CreateCluster, clusterName:%s", clusterName)
	}

	createTime := time.Now().UnixMilli()
	clusterMetadataStorage := storage.Cluster{
		ID:             clusterID,
		Name:           clusterName,
		MinNodeCount:   opts.NodeCount,
		ShardTotal:     opts.ShardTotal,
		EnableSchedule: opts.EnableSchedule,
		TopologyType:   opts.TopologyType,
		CreatedAt:      uint64(createTime),
		ModifiedAt:     uint64(createTime),
	}
	err = m.storage.CreateCluster(ctx, storage.CreateClusterRequest{
		Cluster: clusterMetadataStorage,
	})
	if err != nil {
		log.Error("fail to create cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return nil, errors.WithMessage(err, "cluster create cluster")
	}

	logger := log.With(zap.String("clusterName", clusterName))

	clusterMetadata := metadata.NewClusterMetadata(logger, clusterMetadataStorage, m.storage, m.kv, m.rootPath, m.idAllocatorStep)

	if err = clusterMetadata.Init(ctx); err != nil {
		log.Error("fail to init cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return nil, errors.WithMessage(err, "cluster init")
	}

	if err := clusterMetadata.Load(ctx); err != nil {
		log.Error("fail to load cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return nil, errors.WithMessage(err, "cluster load")
	}

	c, err := NewCluster(logger, clusterMetadata, m.client, m.rootPath)
	if err != nil {
		return nil, errors.WithMessage(err, "new cluster")
	}
	m.clusters[clusterName] = c

	if err := c.Start(ctx); err != nil {
		return nil, errors.WithMessage(err, "start cluster")
	}

	return c, nil
}

func (m *managerImpl) UpdateCluster(ctx context.Context, clusterName string, opt metadata.UpdateClusterOpts) error {
	c, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err))
		return err
	}

	err = m.storage.UpdateCluster(ctx, storage.UpdateClusterRequest{Cluster: storage.Cluster{
		ID:             c.GetMetadata().GetClusterID(),
		Name:           c.GetMetadata().Name(),
		MinNodeCount:   c.GetMetadata().GetClusterMinNodeCount(),
		ShardTotal:     c.GetMetadata().GetTotalShardNum(),
		EnableSchedule: opt.EnableSchedule,
		TopologyType:   opt.TopologyType,
		CreatedAt:      c.GetMetadata().GetCreateTime(),
		ModifiedAt:     uint64(time.Now().UnixMilli()),
	}})
	if err != nil {
		log.Error("update cluster", zap.Error(err))
		return err
	}

	if err := c.GetMetadata().LoadMetadata(ctx); err != nil {
		log.Error("fail to load cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return err
	}

	return nil
}

func (m *managerImpl) GetCluster(_ context.Context, clusterName string) (*Cluster, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	cluster, exist := m.clusters[clusterName]
	if exist {
		return cluster, nil
	}
	return nil, metadata.ErrClusterNotFound
}

func (m *managerImpl) AllocSchemaID(ctx context.Context, clusterName, schemaName string) (storage.SchemaID, bool, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return 0, false, errors.WithMessage(err, "get cluster")
	}

	// create new schema
	schema, exists, err := cluster.metadata.GetOrCreateSchema(ctx, schemaName)
	if err != nil {
		log.Error("fail to create schema", zap.Error(err))
		return 0, false, errors.WithMessage(err, "get or create schema")
	}
	return schema.ID, exists, nil
}

func (m *managerImpl) GetTables(clusterName, _ string, shardIDs []storage.ShardID) (map[storage.ShardID]metadata.ShardTables, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		return nil, errors.WithMessage(err, "get cluster")
	}

	shardTables := cluster.metadata.GetShardTables(shardIDs)
	return shardTables, nil
}

func (m *managerImpl) DropTable(ctx context.Context, clusterName, schemaName, tableName string) error {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return errors.WithMessage(err, "get cluster")
	}

	_, err = cluster.metadata.DropTable(ctx, schemaName, tableName)
	if err != nil {
		return errors.WithMessage(err, "cluster drop table")
	}

	return nil
}

func (m *managerImpl) RegisterNode(ctx context.Context, clusterName string, registeredNode metadata.RegisteredNode) error {
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

	err = cluster.metadata.RegisterNode(ctx, registeredNode)

	if err != nil {
		return errors.WithMessage(err, "cluster register node")
	}

	return nil
}

func (m *managerImpl) GetRegisteredNode(_ context.Context, clusterName string, nodeName string) (metadata.RegisteredNode, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return metadata.RegisteredNode{}, errors.WithMessage(err, "get cluster")
	}

	registeredNode, ok := cluster.metadata.GetRegisteredNodeByName(nodeName)
	if !ok {
		return metadata.RegisteredNode{}, metadata.ErrNodeNotFound.WithCausef("registeredNode is not found, registeredNode:%s, cluster:%s", nodeName, clusterName)
	}

	return registeredNode, nil
}

func (m *managerImpl) ListRegisterNodes(_ context.Context, clusterName string) ([]metadata.RegisteredNode, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return []metadata.RegisteredNode{}, errors.WithMessage(err, "get cluster")
	}

	nodes := cluster.metadata.GetRegisteredNodes()
	return nodes, nil
}

func (m *managerImpl) getCluster(clusterName string) (*Cluster, error) {
	m.lock.RLock()
	cluster, ok := m.clusters[clusterName]
	m.lock.RUnlock()
	if !ok {
		return nil, metadata.ErrClusterNotFound.WithCausef("cluster name:%s", clusterName)
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
	for _, metadataStorage := range clusters.Clusters {
		logger := log.With(zap.String("clusterName", metadataStorage.Name))
		clusterMetadata := metadata.NewClusterMetadata(logger, metadataStorage, m.storage, m.kv, m.rootPath, m.idAllocatorStep)
		if err = clusterMetadata.Load(ctx); err != nil {
			log.Error("fail to load cluster", zap.String("cluster", clusterMetadata.Name()), zap.Error(err))
			return errors.WithMessage(err, "fail to load cluster")
		}

		// TODO: topologyType is used to be compatible with cluster data changes and needs to be deleted later
		if clusterMetadata.GetStorageMetadata().TopologyType == storage.TopologyTypeUnknown {
			req := storage.UpdateClusterRequest{
				Cluster: storage.Cluster{
					ID:             metadataStorage.ID,
					Name:           metadataStorage.Name,
					MinNodeCount:   metadataStorage.MinNodeCount,
					ShardTotal:     metadataStorage.ShardTotal,
					EnableSchedule: metadataStorage.EnableSchedule,
					TopologyType:   m.topologyType,
					CreatedAt:      metadataStorage.CreatedAt,
					ModifiedAt:     uint64(time.Now().UnixMilli()),
				},
			}
			if err := m.storage.UpdateCluster(ctx, req); err != nil {
				return errors.WithMessagef(err, "update cluster topology type failed, clusterName:%s", clusterMetadata.Name())
			}
			log.Info("update cluster topology type successfully", zap.String("request", fmt.Sprintf("%v", req)))
			if err := clusterMetadata.LoadMetadata(ctx); err != nil {
				log.Error("fail to load cluster", zap.String("clusterName", clusterMetadata.Name()), zap.Error(err))
				return err
			}
		}

		log.Info("open cluster successfully", zap.String("cluster", clusterMetadata.Name()))
		c, err := NewCluster(logger, clusterMetadata, m.client, m.rootPath)
		if err != nil {
			return errors.WithMessage(err, "new cluster")
		}
		m.clusters[clusterMetadata.Name()] = c
		if err := c.Start(ctx); err != nil {
			return errors.WithMessage(err, "start cluster")
		}
	}

	m.running = true

	return nil
}

func (m *managerImpl) Stop(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.running {
		return nil
	}

	for _, cluster := range m.clusters {
		if err := cluster.Stop(ctx); err != nil {
			return errors.WithMessage(err, "stop cluster")
		}
	}

	m.clusters = make(map[string]*Cluster)
	m.running = false
	return nil
}

func (m *managerImpl) RouteTables(ctx context.Context, clusterName, schemaName string, tableNames []string) (metadata.RouteTablesResult, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return metadata.RouteTablesResult{}, errors.WithMessage(err, "get cluster")
	}

	ret, err := cluster.metadata.RouteTables(ctx, schemaName, tableNames)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return metadata.RouteTablesResult{}, errors.WithMessage(err, "cluster route tables")
	}

	return ret, nil
}

func (m *managerImpl) GetNodeShards(ctx context.Context, clusterName string) (metadata.GetNodeShardsResult, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("get cluster", zap.Error(err), zap.String("clusterName", clusterName))
		return metadata.GetNodeShardsResult{}, errors.WithMessage(err, "get cluster")
	}

	ret, err := cluster.metadata.GetNodeShards(ctx)
	if err != nil {
		return metadata.GetNodeShardsResult{}, errors.WithMessage(err, "cluster get NodeShards")
	}

	return ret, nil
}
