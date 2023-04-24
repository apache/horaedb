// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package metadata

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"path"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	AllocSchemaIDPrefix = "SchemaID"
	AllocTableIDPrefix  = "TableID"
)

type ClusterMetadata struct {
	clusterID storage.ClusterID

	// RWMutex is used to protect following fields.
	// TODO: Encapsulated maps as a specific struct.
	lock     sync.RWMutex
	metaData storage.Cluster

	tableManager    TableManager
	topologyManager TopologyManager

	// Manage the registered nodes from heartbeat.
	registeredNodesCache map[string]RegisteredNode // nodeName -> NodeName

	storage      storage.Storage
	kv           clientv3.KV
	shardIDAlloc id.Allocator
}

func NewClusterMetadata(meta storage.Cluster, storage storage.Storage, kv clientv3.KV, rootPath string, idAllocatorStep uint) *ClusterMetadata {
	schemaIDAlloc := id.NewAllocatorImpl(kv, path.Join(rootPath, meta.Name, AllocSchemaIDPrefix), idAllocatorStep)
	tableIDAlloc := id.NewAllocatorImpl(kv, path.Join(rootPath, meta.Name, AllocTableIDPrefix), idAllocatorStep)
	// FIXME: Load ShardTopology when cluster create, pass exist ShardID to allocator.
	shardIDAlloc := id.NewReusableAllocatorImpl([]uint64{}, MinShardID)

	cluster := &ClusterMetadata{
		clusterID:            meta.ID,
		metaData:             meta,
		tableManager:         NewTableManagerImpl(storage, meta.ID, schemaIDAlloc, tableIDAlloc),
		topologyManager:      NewTopologyManagerImpl(storage, meta.ID, shardIDAlloc),
		registeredNodesCache: map[string]RegisteredNode{},
		storage:              storage,
		kv:                   kv,
		shardIDAlloc:         shardIDAlloc,
	}

	return cluster
}

func (c *ClusterMetadata) GetClusterID() storage.ClusterID {
	return c.clusterID
}

func (c *ClusterMetadata) Name() string {
	return c.metaData.Name
}

func (c *ClusterMetadata) GetShardTables(shardIDs []storage.ShardID) map[storage.ShardID]ShardTables {
	shardTableIDs := c.topologyManager.GetTableIDs(shardIDs)

	result := make(map[storage.ShardID]ShardTables, len(shardIDs))

	schemas := c.tableManager.GetSchemas()
	schemaByID := make(map[storage.SchemaID]storage.Schema)
	for _, schema := range schemas {
		schemaByID[schema.ID] = schema
	}

	for shardID, shardTableID := range shardTableIDs {
		tables := c.tableManager.GetTablesByIDs(shardTableID.TableIDs)
		tableInfos := make([]TableInfo, 0, len(tables))
		for _, table := range tables {
			schema, ok := schemaByID[table.SchemaID]
			if !ok {
				log.Warn("schema not exits", zap.Uint64("schemaID", uint64(table.SchemaID)))
			}
			tableInfos = append(tableInfos, TableInfo{
				ID:            table.ID,
				Name:          table.Name,
				SchemaID:      table.SchemaID,
				SchemaName:    schema.Name,
				PartitionInfo: table.PartitionInfo,
			})
		}
		result[shardID] = ShardTables{
			Shard: ShardInfo{
				ID:      shardTableID.ShardNode.ID,
				Role:    shardTableID.ShardNode.ShardRole,
				Version: shardTableID.Version,
			},
			Tables: tableInfos,
		}
	}

	for _, shardID := range shardIDs {
		_, exists := result[shardID]
		if !exists {
			result[shardID] = ShardTables{
				Shard: ShardInfo{
					ID:      shardID,
					Role:    storage.ShardRoleLeader,
					Version: 0,
				},
				Tables: []TableInfo{},
			}
		}
	}
	return result
}

// DropTable will drop table metadata and all mapping of this table.
// If the table to be dropped has been opened multiple times, all its mapping will be dropped.
func (c *ClusterMetadata) DropTable(ctx context.Context, schemaName, tableName string) (DropTableResult, error) {
	log.Info("drop table start", zap.String("cluster", c.Name()), zap.String("schemaName", schemaName), zap.String("tableName", tableName))

	table, ok, err := c.tableManager.GetTable(schemaName, tableName)
	if err != nil {
		return DropTableResult{}, errors.WithMessage(err, "get table")
	}

	if !ok {
		return DropTableResult{}, ErrTableNotFound
	}

	// Drop table.
	err = c.tableManager.DropTable(ctx, schemaName, tableName)
	if err != nil {
		return DropTableResult{}, errors.WithMessage(err, "table manager drop table")
	}

	// Remove dropped table in shard view.
	updateVersions, err := c.topologyManager.EvictTable(ctx, table.ID)
	if err != nil {
		return DropTableResult{}, errors.WithMessage(err, "topology manager remove table")
	}

	ret := DropTableResult{
		ShardVersionUpdate: updateVersions,
	}
	log.Info("drop table success", zap.String("cluster", c.Name()), zap.String("schemaName", schemaName), zap.String("tableName", tableName), zap.String("result", fmt.Sprintf("%+v", ret)))

	return ret, nil
}

// OpenTable will open an existing table on the specified shard.
// The table to be opened must have been created.
func (c *ClusterMetadata) OpenTable(ctx context.Context, request OpenTableRequest) (ShardVersionUpdate, error) {
	log.Info("open table", zap.String("request", fmt.Sprintf("%v", request)))

	table, exists, err := c.GetTable(request.SchemaName, request.TableName)
	if err != nil {
		log.Error("get table", zap.Error(err), zap.String("schemaName", request.SchemaName), zap.String("tableName", request.TableName))
		return ShardVersionUpdate{}, err
	}

	if !exists {
		log.Error("the table to be opened does not exist", zap.String("schemaName", request.SchemaName), zap.String("tableName", request.TableName))
		return ShardVersionUpdate{}, errors.WithMessagef(ErrTableNotFound, "table not exists, shcemaName:%s,tableName:%s", request.SchemaName, request.TableName)
	}

	if !table.IsPartitioned() {
		log.Error("normal table cannot be opened on multiple shards", zap.String("schemaName", request.SchemaName), zap.String("tableName", request.TableName))
		return ShardVersionUpdate{}, errors.WithMessagef(ErrOpenTable, "normal table cannot be opened on multiple shards, schemaName:%s, tableName:%s", request.SchemaName, request.TableName)
	}

	shardVersionUpdate, err := c.topologyManager.AddTable(ctx, request.ShardID, []storage.Table{table})
	if err != nil {
		return ShardVersionUpdate{}, errors.WithMessage(err, "add table to topology")
	}

	log.Info("open table finish", zap.String("request", fmt.Sprintf("%v", request)))
	return shardVersionUpdate, nil
}

func (c *ClusterMetadata) CloseTable(ctx context.Context, request CloseTableRequest) (ShardVersionUpdate, error) {
	log.Info("close table", zap.String("request", fmt.Sprintf("%v", request)))

	table, exists, err := c.GetTable(request.SchemaName, request.TableName)
	if err != nil {
		log.Error("get table", zap.Error(err), zap.String("schemaName", request.SchemaName), zap.String("tableName", request.TableName))
		return ShardVersionUpdate{}, err
	}

	if !exists {
		log.Error("the table to be closed does not exist", zap.String("schemaName", request.SchemaName), zap.String("tableName", request.TableName))
		return ShardVersionUpdate{}, errors.WithMessagef(ErrTableNotFound, "table not exists, shcemaName:%s, tableName:%s", request.SchemaName, request.TableName)
	}

	shardVersionUpdate, err := c.topologyManager.RemoveTable(ctx, request.ShardID, []storage.TableID{table.ID})
	if err != nil {
		return ShardVersionUpdate{}, err
	}

	log.Info("close table finish", zap.String("request", fmt.Sprintf("%v", request)))
	return shardVersionUpdate, nil
}

// MigrateTable used to migrate tables from old shard to new shard.
// The mapping relationship between table and shard will be modified.
func (c *ClusterMetadata) MigrateTable(ctx context.Context, request MigrateTableRequest) error {
	log.Info("migrate table", zap.String("request", fmt.Sprintf("%v", request)))

	tables := make([]storage.Table, 0, len(request.TableNames))
	tableIDs := make([]storage.TableID, 0, len(request.TableNames))

	for _, tableName := range request.TableNames {
		table, exists, err := c.tableManager.GetTable(request.SchemaName, tableName)
		if err != nil {
			log.Error("get table", zap.Error(err), zap.String("schemaName", request.SchemaName), zap.String("tableName", tableName))
			return err
		}

		if !exists {
			log.Error("the table to be closed does not exist", zap.String("schemaName", request.SchemaName), zap.String("tableName", tableName))
			return errors.WithMessagef(ErrTableNotFound, "table not exists, shcemaName:%s,tableName:%s", request.SchemaName, tableName)
		}

		tables = append(tables, table)
		tableIDs = append(tableIDs, table.ID)
	}

	if _, err := c.topologyManager.RemoveTable(ctx, request.OldShardID, tableIDs); err != nil {
		log.Error("remove table from topology")
		return err
	}

	if _, err := c.topologyManager.AddTable(ctx, request.NewShardID, tables); err != nil {
		log.Error("add table from topology")
		return err
	}

	log.Info("migrate table finish", zap.String("request", fmt.Sprintf("%v", request)))
	return nil
}

// GetOrCreateSchema the second output parameter bool: returns true if the schema was newly created.
func (c *ClusterMetadata) GetOrCreateSchema(ctx context.Context, schemaName string) (storage.Schema, bool, error) {
	return c.tableManager.GetOrCreateSchema(ctx, schemaName)
}

// GetTable the second output parameter bool: returns true if the table exists.
func (c *ClusterMetadata) GetTable(schemaName, tableName string) (storage.Table, bool, error) {
	return c.tableManager.GetTable(schemaName, tableName)
}

func (c *ClusterMetadata) CreateTableMetadata(ctx context.Context, request CreateTableMetadataRequest) (CreateTableMetadataResult, error) {
	log.Info("create table start", zap.String("cluster", c.Name()), zap.String("schemaName", request.SchemaName), zap.String("tableName", request.TableName))

	_, exists, err := c.tableManager.GetTable(request.SchemaName, request.TableName)
	if err != nil {
		return CreateTableMetadataResult{}, err
	}

	if exists {
		return CreateTableMetadataResult{}, ErrTableAlreadyExists
	}

	// Create table in table manager.
	table, err := c.tableManager.CreateTable(ctx, request.SchemaName, request.TableName, request.PartitionInfo)
	if err != nil {
		return CreateTableMetadataResult{}, errors.WithMessage(err, "table manager create table")
	}

	res := CreateTableMetadataResult{
		Table: table,
	}

	log.Info("create table metadata succeed", zap.String("cluster", c.Name()), zap.String("result", fmt.Sprintf("%+v", res)))
	return res, nil
}

func (c *ClusterMetadata) AddTableTopology(ctx context.Context, shardID storage.ShardID, table storage.Table) (CreateTableResult, error) {
	log.Info("add table topology start", zap.String("cluster", c.Name()), zap.String("tableName", table.Name))

	// Add table to topology manager.
	result, err := c.topologyManager.AddTable(ctx, shardID, []storage.Table{table})
	if err != nil {
		return CreateTableResult{}, errors.WithMessage(err, "topology manager add table")
	}

	ret := CreateTableResult{
		Table:              table,
		ShardVersionUpdate: result,
	}
	log.Info("add table topology succeed", zap.String("cluster", c.Name()), zap.String("result", fmt.Sprintf("%+v", ret)))
	return ret, nil
}

func (c *ClusterMetadata) DropTableMetadata(ctx context.Context, schemaName, tableName string) (DropTableMetadataResult, error) {
	log.Info("drop table start", zap.String("cluster", c.Name()), zap.String("schemaName", schemaName), zap.String("tableName", tableName))

	table, ok, err := c.tableManager.GetTable(schemaName, tableName)
	if err != nil {
		return DropTableMetadataResult{}, errors.WithMessage(err, "get table")
	}

	if !ok {
		return DropTableMetadataResult{}, ErrTableNotFound
	}

	err = c.tableManager.DropTable(ctx, schemaName, tableName)
	if err != nil {
		return DropTableMetadataResult{}, errors.WithMessage(err, "table manager drop table")
	}

	log.Info("drop table metadata success", zap.String("cluster", c.Name()), zap.String("schemaName", schemaName), zap.String("tableName", tableName), zap.String("result", fmt.Sprintf("%+v", table)))

	return DropTableMetadataResult{Table: table}, nil
}

func (c *ClusterMetadata) CreateTable(ctx context.Context, request CreateTableRequest) (CreateTableResult, error) {
	log.Info("create table start", zap.String("cluster", c.Name()), zap.String("schemaName", request.SchemaName), zap.String("tableName", request.TableName))

	_, exists, err := c.tableManager.GetTable(request.SchemaName, request.TableName)
	if err != nil {
		return CreateTableResult{}, err
	}

	if exists {
		return CreateTableResult{}, ErrTableAlreadyExists
	}

	// Create table in table manager.
	table, err := c.tableManager.CreateTable(ctx, request.SchemaName, request.TableName, request.PartitionInfo)
	if err != nil {
		return CreateTableResult{}, errors.WithMessage(err, "table manager create table")
	}

	// Add table to topology manager.
	result, err := c.topologyManager.AddTable(ctx, request.ShardID, []storage.Table{table})
	if err != nil {
		return CreateTableResult{}, errors.WithMessage(err, "topology manager add table")
	}

	ret := CreateTableResult{
		Table:              table,
		ShardVersionUpdate: result,
	}
	log.Info("create table succeed", zap.String("cluster", c.Name()), zap.String("result", fmt.Sprintf("%+v", ret)))
	return ret, nil
}

func (c *ClusterMetadata) GetShardNodesByShardID(id storage.ShardID) ([]storage.ShardNode, error) {
	return c.topologyManager.GetShardNodesByID(id)
}

func (c *ClusterMetadata) GetShardNodeByTableIDs(tableIDs []storage.TableID) (GetShardNodesByTableIDsResult, error) {
	return c.topologyManager.GetShardNodesByTableIDs(tableIDs)
}

func (c *ClusterMetadata) RegisterNode(ctx context.Context, registeredNode RegisteredNode) error {
	registeredNode.Node.State = storage.NodeStateOnline
	err := c.storage.CreateOrUpdateNode(ctx, storage.CreateOrUpdateNodeRequest{
		ClusterID: c.clusterID,
		Node:      registeredNode.Node,
	})
	if err != nil {
		return errors.WithMessage(err, "create or update registered node")
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	c.registeredNodesCache[registeredNode.Node.Name] = registeredNode

	// When the number of nodes in the cluster reaches the threshold, modify the cluster status to stable.
	// TODO: Consider the design of the entire cluster state, which may require refactoring.
	if uint32(len(c.registeredNodesCache)) >= c.metaData.MinNodeCount && c.topologyManager.GetClusterState() == storage.ClusterStateEmpty {
		if err := c.UpdateClusterView(ctx, storage.ClusterStateStable, []storage.ShardNode{}); err != nil {
			log.Error("update cluster view failed", zap.Error(err))
		}
	}

	// Update shard node mapping.
	shardNodes := make(map[string][]storage.ShardNode, len(registeredNode.ShardInfos))
	for _, shardInfo := range registeredNode.ShardInfos {
		shardNodes[registeredNode.Node.Name] = append(shardNodes[registeredNode.Node.Name], storage.ShardNode{
			ID:        shardInfo.ID,
			ShardRole: shardInfo.Role,
			NodeName:  registeredNode.Node.Name,
		})
	}
	if err := c.UpdateClusterViewByNode(ctx, shardNodes); err != nil {
		return errors.WithMessage(err, "update cluster view failed")
	}

	return nil
}

func (c *ClusterMetadata) GetRegisteredNodes() []RegisteredNode {
	c.lock.RLock()
	defer c.lock.RUnlock()

	nodes := make([]RegisteredNode, 0, len(c.registeredNodesCache))
	for _, node := range c.registeredNodesCache {
		nodes = append(nodes, node)
	}
	return nodes
}

func (c *ClusterMetadata) GetRegisteredNodeByName(nodeName string) (RegisteredNode, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	registeredNode, ok := c.registeredNodesCache[nodeName]
	return registeredNode, ok
}

func (c *ClusterMetadata) AllocShardID(ctx context.Context) (uint32, error) {
	id, err := c.shardIDAlloc.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessage(err, "alloc shard id")
	}
	return uint32(id), nil
}

func (c *ClusterMetadata) RouteTables(_ context.Context, schemaName string, tableNames []string) (RouteTablesResult, error) {
	routeEntries := make(map[string]RouteEntry, len(tableNames))
	tables := make(map[storage.TableID]storage.Table, len(tableNames))
	tableIDs := make([]storage.TableID, 0, len(tableNames))
	for _, tableName := range tableNames {
		table, exists, err := c.tableManager.GetTable(schemaName, tableName)
		if err != nil {
			return RouteTablesResult{}, errors.WithMessage(err, "table manager get table")
		}
		if exists {
			// TODO: Adapt to the current implementation of the partition table, which may need to be reconstructed later.
			if !table.IsPartitioned() {
				tables[table.ID] = table
				tableIDs = append(tableIDs, table.ID)
			} else {
				routeEntries[table.Name] = RouteEntry{
					Table: TableInfo{
						ID:            table.ID,
						Name:          table.Name,
						SchemaID:      table.SchemaID,
						SchemaName:    schemaName,
						PartitionInfo: table.PartitionInfo,
					},
					NodeShards: nil,
				}
			}
		}
	}

	tableShardNodesWithShardViewVersion, err := c.topologyManager.GetShardNodesByTableIDs(tableIDs)
	if err != nil {
		return RouteTablesResult{}, errors.WithMessage(err, "topology get shard nodes by table ids")
	}
	for tableID, value := range tableShardNodesWithShardViewVersion.ShardNodes {
		nodeShards := make([]ShardNodeWithVersion, 0, len(value))
		for _, shardNode := range value {
			nodeShards = append(nodeShards, ShardNodeWithVersion{
				ShardInfo: ShardInfo{
					ID:      shardNode.ID,
					Role:    shardNode.ShardRole,
					Version: tableShardNodesWithShardViewVersion.Version[shardNode.ID],
				},
				ShardNode: shardNode,
			})
		}
		// If nodeShards length bigger than 1, randomly select a nodeShard.
		nodeShardsResult := nodeShards
		if len(nodeShards) > 1 {
			selectIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(nodeShards))))
			if err != nil {
				return RouteTablesResult{}, errors.WithMessage(err, "generate random node index")
			}
			nodeShardsResult = []ShardNodeWithVersion{nodeShards[selectIndex.Uint64()]}
		}
		table := tables[tableID]
		routeEntries[table.Name] = RouteEntry{
			Table: TableInfo{
				ID:            table.ID,
				Name:          table.Name,
				SchemaID:      table.SchemaID,
				SchemaName:    schemaName,
				PartitionInfo: table.PartitionInfo,
			},
			NodeShards: nodeShardsResult,
		}
	}
	return RouteTablesResult{
		ClusterViewVersion: c.topologyManager.GetVersion(),
		RouteEntries:       routeEntries,
	}, nil
}

func (c *ClusterMetadata) GetNodeShards(_ context.Context) (GetNodeShardsResult, error) {
	getNodeShardsResult := c.topologyManager.GetShardNodes()

	shardNodesWithVersion := make([]ShardNodeWithVersion, 0, len(getNodeShardsResult.shardNodes))

	for _, shardNode := range getNodeShardsResult.shardNodes {
		shardNodesWithVersion = append(shardNodesWithVersion, ShardNodeWithVersion{
			ShardInfo: ShardInfo{
				ID:      shardNode.ID,
				Role:    shardNode.ShardRole,
				Version: getNodeShardsResult.versions[shardNode.ID],
			},
			ShardNode: shardNode,
		})
	}

	return GetNodeShardsResult{
		ClusterTopologyVersion: c.topologyManager.GetVersion(),
		NodeShards:             shardNodesWithVersion,
	}, nil
}

func (c *ClusterMetadata) GetClusterViewVersion() uint64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.topologyManager.GetVersion()
}

func (c *ClusterMetadata) GetClusterMinNodeCount() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.MinNodeCount
}

func (c *ClusterMetadata) GetTotalShardNum() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.ShardTotal
}

func (c *ClusterMetadata) GetClusterState() storage.ClusterState {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.topologyManager.GetClusterState()
}

func (c *ClusterMetadata) GetClusterView() storage.ClusterView {
	return c.topologyManager.GetClusterView()
}

func (c *ClusterMetadata) UpdateClusterView(ctx context.Context, state storage.ClusterState, shardNodes []storage.ShardNode) error {
	if err := c.topologyManager.UpdateClusterView(ctx, state, shardNodes); err != nil {
		return errors.WithMessage(err, "update cluster view")
	}
	return nil
}

func (c *ClusterMetadata) UpdateClusterViewByNode(ctx context.Context, shardNodes map[string][]storage.ShardNode) error {
	if err := c.topologyManager.UpdateClusterViewByNode(ctx, shardNodes); err != nil {
		return errors.WithMessage(err, "update cluster view")
	}
	return nil
}

func (c *ClusterMetadata) DropShardNode(ctx context.Context, shardNodes []storage.ShardNode) error {
	if err := c.topologyManager.DropShardNodes(ctx, shardNodes); err != nil {
		return errors.WithMessage(err, "drop shard nodes")
	}
	return nil
}

func (c *ClusterMetadata) CreateShardViews(ctx context.Context, views []CreateShardView) error {
	if err := c.topologyManager.CreateShardViews(ctx, views); err != nil {
		return errors.WithMessage(err, "topology manager create shard views")
	}

	return nil
}

func (c *ClusterMetadata) GetClusterSnapshot() Snapshot {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return Snapshot{
		Topology:        c.topologyManager.GetTopology(),
		RegisteredNodes: c.GetRegisteredNodes(),
	}
}

// Initialize the cluster view and shard view of the cluster.
// It will be used when we create the cluster.
func (c *ClusterMetadata) Init(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	createShardViews := make([]CreateShardView, 0, c.metaData.ShardTotal)
	for i := uint32(0); i < c.metaData.ShardTotal; i++ {
		shardID, err := c.AllocShardID(ctx)
		if err != nil {
			return errors.WithMessage(err, "alloc shard id failed")
		}
		createShardViews = append(createShardViews, CreateShardView{
			ShardID: storage.ShardID(shardID),
			Tables:  []storage.TableID{},
		})
	}
	if err := c.topologyManager.CreateShardViews(ctx, createShardViews); err != nil {
		return errors.WithMessage(err, "create shard view")
	}

	return c.topologyManager.InitClusterView(ctx)
}

// Load cluster NodeName from storage into memory.
func (c *ClusterMetadata) Load(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if err := c.tableManager.Load(ctx); err != nil {
		return errors.WithMessage(err, "load table manager")
	}

	if err := c.topologyManager.Load(ctx); err != nil {
		return errors.WithMessage(err, "load topology manager")
	}

	return nil
}
