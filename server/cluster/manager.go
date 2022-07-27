// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

type TableInfo struct {
	id         uint64
	name       string
	schemaID   uint32
	schemaName string
}

type ShardTables struct {
	shardRole clusterpb.ShardRole
	tables    []*TableInfo
	version   uint64
}

type Manager interface {
	CreateCluster(ctx context.Context, clusterName string, nodeCount, replicationFactor, shardTotal uint32) (*Cluster, error)
	AllocSchemaID(ctx context.Context, clusterName, schemaName string) (uint32, error)
	AllocTableID(ctx context.Context, clusterName, schemaName, tableName, nodeName string) (uint64, error)
	GetTables(ctx context.Context, clusterName, nodeName string, shardIDs []uint32) (map[uint32]*ShardTables, error)
	DropTable(ctx context.Context, clusterName, schemaName, tableName string, tableID uint64) error
	RegisterNode(ctx context.Context, clusterName, nodeName string, lease uint32) error
	GetShards(ctx context.Context, clusterName, nodeName string) ([]uint32, error)
}

type managerImpl struct {
	// RWMutex is used to protect clusters when creating new cluster
	lock     sync.RWMutex
	clusters map[string]*Cluster

	storage storage.Storage
}

func NewManagerImpl(storage storage.Storage) Manager {
	return &managerImpl{storage: storage, clusters: make(map[string]*Cluster, 0)}
}

func (m *managerImpl) Load(ctx context.Context) error {
	clusters, err := m.storage.ListClusters(ctx)
	if err != nil {
		return errors.Wrap(err, "clusters manager Load")
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	m.clusters = make(map[string]*Cluster, len(clusters))
	for _, clusterPb := range clusters {
		cluster := NewCluster(clusterPb, m.storage)
		if err := cluster.Load(ctx); err != nil {
			return errors.Wrapf(err, "clusters manager Load, clusters:%v", cluster)
		}
		m.clusters[cluster.Name()] = cluster
	}
	return nil
}

func (m *managerImpl) CreateCluster(ctx context.Context, clusterName string, nodeCount,
	replicationFactor, shardTotal uint32,
) (*Cluster, error) {
	if nodeCount < 1 {
		return nil, ErrCreateCluster.WithCausef("nodeCount must > 0")
	}

	m.lock.Lock()

	_, ok := m.clusters[clusterName]
	if ok {
		return nil, ErrClusterAlreadyExists
	}

	clusterID, err := m.allocClusterID()
	if err != nil {
		return nil, errors.Wrapf(err, "clusters manager CreateCluster, clusterName:%s", clusterName)
	}

	clusterPb := &clusterpb.Cluster{
		Id: clusterID, Name: clusterName, MinNodeCount: nodeCount,
		ReplicationFactor: replicationFactor, ShardTotal: shardTotal,
	}
	clusterPb, err = m.storage.CreateCluster(ctx, clusterPb)
	if err != nil {
		return nil, errors.Wrapf(err, "clusters manager CreateCluster, clusters:%v", clusterPb)
	}

	// TODO: add scheduler
	clusterTopologyPb := &clusterpb.ClusterTopology{
		ClusterId: clusterID, DataVersion: 0,
		State: clusterpb.ClusterTopology_STABLE,
	}
	if err := m.storage.CreateClusterTopology(ctx, clusterTopologyPb); err != nil {
		return nil, errors.Wrapf(err, "clusters manager CreateCluster, clusterTopology:%v", clusterTopologyPb)
	}

	cluster := NewCluster(clusterPb, m.storage)
	m.clusters[clusterName] = cluster

	m.lock.Unlock()

	if err := cluster.Load(ctx); err != nil {
		return nil, errors.Wrapf(err, "clusters manager CreateCluster, clusterName:%s", clusterName)
	}

	return cluster, nil
}

func (m *managerImpl) AllocSchemaID(ctx context.Context, clusterName, schemaName string) (uint32, error) {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return 0, errors.Wrap(err, "clusters manager AllocSchemaID")
	}

	schema, exists := cluster.GetSchema(schemaName)
	if exists {
		return schema.GetID(), nil
	}
	// create new schema
	schemaID, err := m.allocSchemaID(clusterName)
	if err != nil {
		return 0, errors.Wrapf(err, "clusters manager AllocSchemaID, "+
			"clusterName:%s, schemaName:%s", clusterName, schemaName)
	}
	if _, err1 := cluster.CreateSchema(ctx, schemaName, schemaID); err1 != nil {
		return 0, errors.Wrapf(err, "clusters manager AllocSchemaID, "+
			"clusterName:%s, schemaName:%s", clusterName, schemaName)
	}
	return schemaID, nil
}

func (m *managerImpl) AllocTableID(ctx context.Context, clusterName, schemaName, tableName, nodeName string) (uint64, error) {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return 0, errors.Wrap(err, "clusters manager AllocTableID")
	}

	table, exists, err := cluster.GetTable(ctx, schemaName, tableName)
	if err != nil {
		return 0, errors.Wrapf(err, "clusters manager AllocTableID, "+
			"clusterName:%s, schemaName:%s, tableName:%s, nodeName:%s", clusterName, schemaName, tableName, nodeName)
	}
	if exists {
		return table.GetID(), nil
	}
	// create new schemasCache
	tableID, err := m.allocTableID(clusterName)
	if err != nil {
		return 0, errors.Wrapf(err, "clusters manager AllocTableID, "+
			"clusterName:%s, schemaName:%s, tableName:%s, nodeName:%s", clusterName, schemaName, tableName, nodeName)
	}
	shardID, err := m.allocShardID(clusterName, nodeName)
	if err != nil {
		return 0, errors.Wrapf(err, "clusters manager AllocTableID, "+
			"clusterName:%s, schemaName:%s, tableName:%s, nodeName:%s", clusterName, schemaName, tableName, nodeName)
	}

	if _, err := cluster.CreateTable(ctx, schemaName, shardID, tableName, tableID); err != nil {
		return 0, errors.Wrapf(err, "clusters manager AllocTableID, "+
			"clusterName:%s, schemaName:%s, tableName:%s, nodeName:%s", clusterName, schemaName, tableName, nodeName)
	}
	return tableID, nil
}

func (m *managerImpl) GetTables(ctx context.Context, clusterName, nodeName string, shardIDs []uint32) (map[uint32]*ShardTables, error) {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return nil, errors.Wrap(err, "clusters manager GetTables")
	}

	shardTablesWithRole, err := cluster.GetTables(ctx, shardIDs, nodeName)
	if err != nil {
		return nil, errors.Wrapf(err, "clusters manager GetTables, "+
			"clusterName:%s, nodeName:%s, shardIDs:%v", clusterName, nodeName, shardIDs)
	}

	ret := make(map[uint32]*ShardTables, len(shardIDs))
	for shardID, shardTables := range shardTablesWithRole {
		tableInfos := make([]*TableInfo, len(shardTables.tables))

		for _, t := range shardTables.tables {
			tableInfos = append(tableInfos, &TableInfo{
				id: t.meta.GetId(), name: t.meta.GetName(),
				schemaID: t.schema.GetId(), schemaName: t.schema.GetName(),
			})
		}
		ret[shardID] = &ShardTables{shardRole: shardTables.shardRole, tables: tableInfos, version: shardTables.version}
	}
	return ret, nil
}

func (m *managerImpl) DropTable(ctx context.Context, clusterName, schemaName, tableName string, tableID uint64) error {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return errors.Wrap(err, "clusters manager DropTable")
	}

	if err := cluster.DropTable(ctx, schemaName, tableName, tableID); err != nil {
		return errors.Wrapf(err, "clusters manager DropTable, clusterName:%s, schemaName:%s, tableName:%s, tableID:%d",
			clusterName, schemaName, tableName, tableID)
	}

	return nil
}

func (m *managerImpl) RegisterNode(ctx context.Context, clusterName, nodeName string, lease uint32) error {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return errors.Wrap(err, "clusters manager RegisterNode")
	}
	err = cluster.RegisterNode(ctx, nodeName, lease)
	if err != nil {
		return errors.Wrap(err, "clusters manager RegisterNode")
	}

	// TODO: refactor coordinator
	if err := cluster.coordinator.Run(ctx); err != nil {
		return errors.Wrap(err, "RegisterNode")
	}
	return nil
}

func (m *managerImpl) GetShards(ctx context.Context, clusterName, nodeName string) ([]uint32, error) {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return nil, errors.Wrap(err, "clusters manager GetShards")
	}

	shardIDs, err := cluster.GetShardIDs(nodeName)
	if err != nil {
		return nil, errors.Wrap(err, "clusters manager GetShards")
	}
	return shardIDs, nil
}

func (m *managerImpl) getCluster(ctx context.Context, clusterName string) (*Cluster, error) {
	m.lock.RLock()
	cluster, ok := m.clusters[clusterName]
	m.lock.RUnlock()
	if !ok {
		return nil, ErrClusterNotFound.WithCausef("clusters manager getCluster, clusterName:%s", clusterName)
	}
	return cluster, nil
}

func (m *managerImpl) allocClusterID() (uint32, error) {
	return 0, nil
}

func (m *managerImpl) allocSchemaID(clusterName string) (uint32, error) {
	return 0, nil
}

func (m *managerImpl) allocTableID(clusterName string) (uint64, error) {
	return 0, nil
}

func (m *managerImpl) allocShardID(clusterName, nodeName string) (uint32, error) {
	return 0, nil
}
