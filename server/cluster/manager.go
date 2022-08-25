// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"path"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/schedule"
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

type TableInfo struct {
	ID         uint64
	Name       string
	SchemaID   uint32
	SchemaName string
}

type ShardTables struct {
	ShardRole clusterpb.ShardRole
	Tables    []*TableInfo
	Version   uint64
}

type ShardInfo struct {
	ShardID   uint32
	ShardRole clusterpb.ShardRole
}

type NodeShard struct {
	Endpoint  string
	ShardInfo *ShardInfo
}

type RouteEntry struct {
	Table      *TableInfo
	NodeShards []*NodeShard
}

type RouteTablesResult struct {
	Version      uint64
	RouteEntries map[string]*RouteEntry
}

type Manager interface {
	// Start must be called before manager is used.
	Start(ctx context.Context) error
	// Stop must be called before manager is dropped.
	Stop(ctx context.Context) error

	CreateCluster(ctx context.Context, clusterName string, nodeCount, replicationFactor, shardTotal uint32) (*Cluster, error)
	AllocSchemaID(ctx context.Context, clusterName, schemaName string) (uint32, error)
	AllocTableID(ctx context.Context, clusterName, schemaName, tableName, nodeName string) (*Table, error)
	GetTables(ctx context.Context, clusterName, nodeName string, shardIDs []uint32) (map[uint32]*ShardTables, error)
	DropTable(ctx context.Context, clusterName, schemaName, tableName string, tableID uint64) error
	RegisterNode(ctx context.Context, clusterName, nodeName string, lease uint32) error
	GetShards(ctx context.Context, clusterName, nodeName string) ([]uint32, error)
	RouteTables(ctx context.Context, clusterName, schemaName string, tableNames []string) (*RouteTablesResult, error)
}

type managerImpl struct {
	// RWMutex is used to protect clusters when creating new cluster.
	lock     sync.RWMutex
	running  bool
	clusters map[string]*Cluster

	storage         storage.Storage
	kv              clientv3.KV
	alloc           id.Allocator
	hbstreams       *schedule.HeartbeatStreams
	rootPath        string
	idAllocatorStep uint
}

func NewManagerImpl(storage storage.Storage, kv clientv3.KV, hbstream *schedule.HeartbeatStreams, rootPath string, idAllocatorStep uint) (Manager, error) {
	alloc := id.NewAllocatorImpl(kv, path.Join(rootPath, AllocClusterIDPrefix), idAllocatorStep)

	manager := &managerImpl{
		storage:         storage,
		kv:              kv,
		alloc:           alloc,
		clusters:        make(map[string]*Cluster, 0),
		hbstreams:       hbstream,
		rootPath:        rootPath,
		idAllocatorStep: idAllocatorStep,
	}

	return manager, nil
}

func (m *managerImpl) CreateCluster(ctx context.Context, clusterName string, initialNodeCount,
	replicationFactor, shardTotal uint32,
) (*Cluster, error) {
	if initialNodeCount < 1 {
		log.Error("cluster's nodeCount must > 0", zap.String("clusterName", clusterName))
		return nil, ErrCreateCluster.WithCausef("nodeCount must > 0")
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	_, ok := m.clusters[clusterName]
	if ok {
		log.Error("cluster already exists", zap.String("clusterName", clusterName))
		return nil, ErrClusterAlreadyExists
	}

	clusterID, err := m.allocClusterID(ctx)
	if err != nil {
		log.Error("fail to alloc cluster id", zap.Error(err))
		return nil, errors.Wrapf(err, "cluster manager CreateCluster, clusterName:%s", clusterName)
	}

	clusterPb := &clusterpb.Cluster{
		Id:                clusterID,
		Name:              clusterName,
		MinNodeCount:      initialNodeCount,
		ReplicationFactor: replicationFactor,
		ShardTotal:        shardTotal,
	}
	clusterPb, err = m.storage.CreateCluster(ctx, clusterPb)
	if err != nil {
		log.Error("fail to create cluster", zap.Error(err))
		return nil, errors.Wrapf(err, "cluster manager CreateCluster, clusters:%v", clusterPb)
	}

	cluster := NewCluster(clusterPb, m.storage, m.kv, m.hbstreams, m.rootPath, m.idAllocatorStep)

	if err = cluster.init(ctx); err != nil {
		log.Error("fail to init cluster", zap.Error(err))
		return nil, errors.Wrapf(err, "cluster manager CreateCluster, clusterName:%s", clusterName)
	}

	if err := cluster.Load(ctx); err != nil {
		log.Error("fail to load cluster", zap.Error(err))
		return nil, errors.Wrapf(err, "cluster manager CreateCluster, clusterName:%s", clusterName)
	}

	m.clusters[clusterName] = cluster

	return cluster, nil
}

func (m *managerImpl) AllocSchemaID(ctx context.Context, clusterName, schemaName string) (uint32, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("cluster not found", zap.Error(err))
		return 0, errors.Wrap(err, "cluster manager AllocSchemaID")
	}

	// create new schema
	schema, err := cluster.GetOrCreateSchema(ctx, schemaName)
	if err != nil {
		log.Error("fail to create schema", zap.Error(err))
		return 0, errors.Wrapf(err, "cluster manager AllocSchemaID, "+
			"clusterName:%s, schemaName:%s", clusterName, schemaName)
	}
	return schema.GetID(), nil
}

func (m *managerImpl) AllocTableID(ctx context.Context, clusterName, schemaName, tableName, nodeName string) (*Table, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("cluster not found", zap.Error(err))
		return nil, errors.Wrap(err, "cluster manager AllocTableID")
	}

	table, err := cluster.GetOrCreateTable(ctx, nodeName, schemaName, tableName)
	if err != nil {
		log.Error("fail to create table", zap.Error(err))
		return nil, errors.Wrapf(err, "cluster manager AllocTableID, "+
			"clusterName:%s, schemaName:%s, tableName:%s, nodeName:%s", clusterName, schemaName, tableName, nodeName)
	}
	return table, nil
}

func (m *managerImpl) GetTables(ctx context.Context, clusterName, nodeName string, shardIDs []uint32) (map[uint32]*ShardTables, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("cluster not found", zap.Error(err))
		return nil, errors.Wrap(err, "cluster manager GetTables")
	}

	shardTablesWithRole, err := cluster.GetTables(ctx, shardIDs, nodeName)
	if err != nil {
		return nil, errors.Wrapf(err, "cluster manager GetTables, "+
			"clusterName:%s, nodeName:%s, shardIDs:%v", clusterName, nodeName, shardIDs)
	}

	ret := make(map[uint32]*ShardTables, len(shardIDs))
	for shardID, shardTables := range shardTablesWithRole {
		tableInfos := make([]*TableInfo, 0, len(shardTables.tables))

		for _, t := range shardTables.tables {
			tableInfos = append(tableInfos, &TableInfo{
				ID: t.meta.GetId(), Name: t.meta.GetName(),
				SchemaID: t.schema.GetId(), SchemaName: t.schema.GetName(),
			})
		}
		ret[shardID] = &ShardTables{ShardRole: shardTables.shardRole, Tables: tableInfos, Version: shardTables.version}
	}
	return ret, nil
}

func (m *managerImpl) DropTable(ctx context.Context, clusterName, schemaName, tableName string, tableID uint64) error {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("cluster not found", zap.Error(err))
		return errors.Wrap(err, "cluster manager DropTable")
	}

	if err := cluster.DropTable(ctx, schemaName, tableName, tableID); err != nil {
		return errors.Wrapf(err, "cluster manager DropTable, clusterName:%s, schemaName:%s, tableName:%s, tableID:%d",
			clusterName, schemaName, tableName, tableID)
	}

	return nil
}

func (m *managerImpl) RegisterNode(ctx context.Context, clusterName, nodeName string, lease uint32) error {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("cluster not found", zap.Error(err))
		return errors.Wrap(err, "cluster manager RegisterNode")
	}
	err = cluster.RegisterNode(ctx, nodeName, lease)
	if err != nil {
		return errors.Wrap(err, "cluster manager RegisterNode")
	}

	// TODO: refactor coordinator
	if err := cluster.coordinator.scatterShard(ctx); err != nil {
		return errors.Wrap(err, "RegisterNode")
	}
	return nil
}

func (m *managerImpl) GetShards(_ context.Context, clusterName, nodeName string) ([]uint32, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("cluster not found", zap.Error(err))
		return nil, errors.Wrap(err, "cluster manager GetShards")
	}

	shardIDs, err := cluster.GetShardIDs(nodeName)
	if err != nil {
		return nil, errors.Wrap(err, "cluster manager GetShards")
	}
	return shardIDs, nil
}

func (m *managerImpl) getCluster(clusterName string) (*Cluster, error) {
	m.lock.RLock()
	cluster, ok := m.clusters[clusterName]
	m.lock.RUnlock()
	if !ok {
		return nil, ErrClusterNotFound.WithCausef("cluster manager getCluster, clusterName:%s", clusterName)
	}
	return cluster, nil
}

func (m *managerImpl) allocClusterID(ctx context.Context) (uint32, error) {
	ID, err := m.alloc.Alloc(ctx)
	if err != nil {
		return 0, errors.Wrapf(err, "alloc cluster id failed")
	}
	return uint32(ID), nil
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
		return errors.Wrap(err, "cluster manager start")
	}

	m.clusters = make(map[string]*Cluster, len(clusters))
	for _, clusterPb := range clusters {
		cluster := NewCluster(clusterPb, m.storage, m.kv, m.hbstreams, m.rootPath, m.idAllocatorStep)
		if err := cluster.Load(ctx); err != nil {
			log.Error("cluster manager fail to start, fail to load cluster", zap.Error(err))
			return errors.Wrapf(err, "cluster manager start, clusters:%v", cluster)
		}
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
	for _, cluster := range m.clusters {
		cluster.stop()
	}

	m.clusters = make(map[string]*Cluster)
	m.running = false
	return nil
}

func (m *managerImpl) RouteTables(ctx context.Context, clusterName, schemaName string, tableNames []string) (*RouteTablesResult, error) {
	cluster, err := m.getCluster(clusterName)
	if err != nil {
		log.Error("cluster not found", zap.Error(err))
		return nil, errors.Wrap(err, "cluster manager routeTables")
	}

	ret, err := cluster.RouteTables(ctx, schemaName, tableNames)
	if err != nil {
		log.Error("cluster manager RouteTables", zap.Error(err))
		return nil, errors.Wrap(err, "cluster manager routeTables")
	}

	return ret, nil
}
