// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"math/rand"
	"path"
	"sync"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type metaData struct {
	cluster         *clusterpb.Cluster
	clusterTopology *clusterpb.ClusterTopology
}

type Cluster struct {
	clusterID uint32

	// RWMutex is used to protect following fields.
	// TODO: Encapsulated maps as a specific struct
	lock         sync.RWMutex
	metaData     *metaData
	shardsCache  map[uint32]*Shard         // shard_id -> shard
	schemasCache map[string]*Schema        // schema_name -> schema
	nodesCache   map[string]*Node          // node_name -> node
	shardLock    map[uint32]*ShardWithLock // shard_id -> shardLock

	storage       storage.Storage
	kv            clientv3.KV
	schemaIDAlloc id.Allocator
	tableIDAlloc  id.Allocator
	shardIDAlloc  id.Allocator
}

func (c *Cluster) GetNodesSize() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return len(c.nodesCache)
}

func (c *Cluster) GetClusterNodeCache() map[string]*Node {
	c.lock.RLock()
	defer c.lock.RUnlock()
	newNodes := make(map[string]*Node)
	for key, value := range c.nodesCache {
		newNodes[key] = value
	}
	return newNodes
}

func (c *Cluster) GetClusterShardView() ([]*clusterpb.Shard, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	shardView := c.metaData.clusterTopology.ShardView
	newShardView := make([]*clusterpb.Shard, 0)
	// TODO: We need to use the general deep copy tool method to replace
	for _, shard := range shardView {
		copyShard := &clusterpb.Shard{
			Id:        shard.Id,
			ShardRole: shard.ShardRole,
			Node:      shard.Node,
		}
		newShardView = append(newShardView, copyShard)
	}
	return newShardView, nil
}

func (c *Cluster) GetClusterID() uint32 {
	return c.clusterID
}

func NewCluster(meta *clusterpb.Cluster, storage storage.Storage, kv clientv3.KV, rootPath string, idAllocatorStep uint) *Cluster {
	cluster := &Cluster{
		clusterID:     meta.GetId(),
		metaData:      &metaData{cluster: meta},
		shardsCache:   make(map[uint32]*Shard),
		schemasCache:  make(map[string]*Schema),
		nodesCache:    make(map[string]*Node),
		schemaIDAlloc: id.NewAllocatorImpl(kv, path.Join(rootPath, meta.Name, AllocSchemaIDPrefix), idAllocatorStep),
		tableIDAlloc:  id.NewAllocatorImpl(kv, path.Join(rootPath, meta.Name, AllocTableIDPrefix), idAllocatorStep),
		// TODO: Load ShardTopology when cluster create, pass exist shardID to allocator
		shardIDAlloc: id.NewReusableAllocatorImpl(make([]uint64, 0), MinShardID),

		storage: storage,
		kv:      kv,
	}

	return cluster
}

func (c *Cluster) Name() string {
	return c.metaData.cluster.Name
}

func (c *Cluster) stop() {
}

// Initialize the cluster topology and shard topology of the cluster.
// It will be used when we create the cluster.
func (c *Cluster) init(ctx context.Context) error {
	clusterTopologyPb := &clusterpb.ClusterTopology{
		ClusterId: c.clusterID, Version: 0,
		State: clusterpb.ClusterTopology_EMPTY,
	}
	if clusterTopologyPb, err := c.storage.CreateClusterTopology(ctx, clusterTopologyPb); err != nil {
		return errors.WithMessagef(err, "cluster init cluster topology, clusterTopology:%v", clusterTopologyPb)
	}

	c.metaData.clusterTopology = clusterTopologyPb

	shardTopologies := make([]*clusterpb.ShardTopology, 0, c.metaData.cluster.ShardTotal)
	for i := uint32(0); i < c.metaData.cluster.ShardTotal; i++ {
		shardTopologies = append(shardTopologies, &clusterpb.ShardTopology{
			ShardId:  i,
			TableIds: make([]uint64, 0),
			Version:  0,
		})
	}
	if shardTopologies, err := c.storage.CreateShardTopologies(ctx, c.clusterID, shardTopologies); err != nil {
		return errors.WithMessagef(err, "cluster init shard topolgies, shardTopologies:%v", shardTopologies)
	}
	return nil
}

// Load data from storage to memory.
func (c *Cluster) Load(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	shards, shardIDs, err := c.loadClusterTopologyLocked(ctx)
	if err != nil {
		return errors.WithMessage(err, "cluster Load")
	}

	shardTopologies, err := c.loadShardTopologyLocked(ctx, shardIDs)
	if err != nil {
		return errors.WithMessage(err, "cluster Load")
	}

	schemas, err := c.loadSchemaLocked(ctx)
	if err != nil {
		return errors.WithMessage(err, "cluster Load")
	}

	nodes, err := c.loadNodeLocked(ctx)
	if err != nil {
		return errors.WithMessage(err, "cluster Load")
	}

	tables, err := c.loadTableLocked(ctx, schemas)
	if err != nil {
		return errors.WithMessage(err, "cluster Load")
	}

	if err := c.loadCacheLocked(shards, shardTopologies, schemas, nodes, tables); err != nil {
		return errors.WithMessage(err, "cluster Load")
	}
	return nil
}

func (c *Cluster) loadCacheLocked(
	shards map[uint32][]*clusterpb.Shard,
	shardTopologies map[uint32]*clusterpb.ShardTopology,
	schemasLoaded map[string]*clusterpb.Schema,
	nodesLoaded map[string]*clusterpb.Node,
	tablesLoaded map[string]map[uint64]*clusterpb.Table,
) error {
	// Load schema data into schema cache.
	for schemaName, tables := range tablesLoaded {
		for _, table := range tables {
			_, ok := c.schemasCache[schemaName]
			if ok {
				c.schemasCache[schemaName].tableMap[table.GetName()] = &Table{
					schema: schemasLoaded[schemaName],
					meta:   table,
				}
			} else {
				c.schemasCache[schemaName] = &Schema{
					meta: schemasLoaded[schemaName],
					tableMap: map[string]*Table{table.GetName(): {
						schema: schemasLoaded[schemaName],
						meta:   table,
					}},
				}
			}
		}
	}

	// Load node data into node cache.
	for shardID, shardPbs := range shards {
		for _, shard := range shardPbs {
			node, ok := c.nodesCache[shard.GetNode()]
			if !ok {
				node = &Node{meta: nodesLoaded[shard.GetNode()]}
				c.nodesCache[shard.GetNode()] = node
			}
			node.shardIDs = append(node.shardIDs, shardID)
		}
	}

	// Load shard data into shard cache.
	for shardID, shardTopology := range shardTopologies {
		tables := make(map[uint64]*Table, len(shardTopology.TableIds))

		for _, tableID := range shardTopology.TableIds {
			for schemaName, tableMap := range tablesLoaded {
				table, ok := tableMap[tableID]
				if ok {
					tables[tableID] = &Table{
						schema: schemasLoaded[schemaName],
						meta:   table,
					}
				}
			}
		}
		// TODO: assert shardID
		// TODO: check shard not found by shardID
		shardMetaList := shards[shardID]
		var nodeMetas []*clusterpb.Node
		for _, shardMeta := range shardMetaList {
			if node := c.nodesCache[shardMeta.Node]; node != nil {
				nodeMetas = append(nodeMetas, node.meta)
			}
		}
		c.shardsCache[shardID] = &Shard{
			meta:    shards[shardID],
			nodes:   nodeMetas,
			tables:  tables,
			version: 0,
		}
	}

	return nil
}

func (c *Cluster) updateSchemaCacheLocked(schemaPb *clusterpb.Schema) *Schema {
	schema := &Schema{meta: schemaPb, tableMap: make(map[string]*Table, 0)}
	c.schemasCache[schemaPb.GetName()] = schema
	return schema
}

func (c *Cluster) updateTableCacheLocked(shardID uint32, schema *Schema, tablePb *clusterpb.Table) *Table {
	table := &Table{meta: tablePb, schema: schema.meta, shardID: shardID}
	schema.tableMap[tablePb.GetName()] = table
	c.shardsCache[tablePb.GetShardId()].tables[table.GetID()] = table
	return table
}

func (c *Cluster) getTableShardIDLocked(tableID uint64) (uint32, error) {
	for id, shard := range c.shardsCache {
		if _, ok := shard.tables[tableID]; ok {
			return id, nil
		}
	}
	return 0, ErrShardNotFound.WithCausef("get table shardID, tableID:%d", tableID)
}

func (c *Cluster) GetTables(_ context.Context, shardIDs []uint32, nodeName string) (map[uint32]*ShardTablesWithRole, error) {
	// TODO: refactor more fine-grained locks
	c.lock.RLock()
	defer c.lock.RUnlock()

	shardTables := make(map[uint32]*ShardTablesWithRole, len(shardIDs))
	for _, shardID := range shardIDs {
		shard, ok := c.shardsCache[shardID]
		if !ok {
			return nil, ErrShardNotFound.WithCausef("shardID:%d", shardID)
		}

		shardRole := clusterpb.ShardRole_FOLLOWER
		found := false
		for i, n := range shard.nodes {
			if nodeName == n.GetName() {
				found = true
				shardRole = shard.meta[i].ShardRole
				break
			}
		}
		if !found {
			return nil, ErrNodeNotFound.WithCausef("nodeName not found in current shard, shardID:%d, nodeName:%s", shardID, nodeName)
		}

		tables := make([]*Table, 0, len(shard.tables))
		for _, table := range shard.tables {
			tables = append(tables, table)
		}
		shardTables[shardID] = &ShardTablesWithRole{shard: &ShardInfo{
			ID:      shardID,
			Role:    shardRole,
			Version: shard.version,
		}, tables: tables}
	}

	return shardTables, nil
}

func (c *Cluster) DropTable(ctx context.Context, schemaName, tableName string, tableID uint64) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	schema, exists := c.getSchemaLocked(schemaName)
	if !exists {
		return ErrSchemaNotFound.WithCausef("schemaName:%s", schemaName)
	}

	if err := c.storage.DeleteTable(ctx, c.clusterID, schema.GetID(), tableName); err != nil {
		return errors.WithMessagef(err, "cluster DropTable, clusterID:%d, schema:%v, tableName:%s",
			c.clusterID, schema, tableName)
	}

	schema.dropTableLocked(tableName)
	for _, shard := range c.shardsCache {
		shard.dropTableLocked(tableID)
	}
	return nil
}

// GetOrCreateSchema the second output parameter bool: Returns true if the schema was newly created.
func (c *Cluster) GetOrCreateSchema(ctx context.Context, schemaName string) (*Schema, bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Check if provided schema exists.
	schema, exists := c.getSchemaLocked(schemaName)
	if exists {
		return schema, true, nil
	}

	schemaID, err := c.allocSchemaID(ctx)
	if err != nil {
		return nil, false, errors.WithMessagef(err, "cluster AllocSchemaID, schemaName:%s", schemaName)
	}

	// Save schema in storage.
	schemaPb := &clusterpb.Schema{Id: schemaID, Name: schemaName, ClusterId: c.clusterID}
	schemaPb, err = c.storage.CreateSchema(ctx, c.clusterID, schemaPb)
	if err != nil {
		return nil, false, errors.WithMessage(err, "cluster CreateSchema")
	}

	// Update schemasCache in memory.
	schema = c.updateSchemaCacheLocked(schemaPb)
	return schema, false, nil
}

// GetOrCreateTable the second output parameter bool: Returns true if the table was newly created.
func (c *Cluster) GetOrCreateTable(ctx context.Context, nodeName string, schemaName string, tableName string) (*Table, bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Check provided schema if exists.
	schema, exists := c.getSchemaLocked(schemaName)
	if !exists {
		return nil, false, ErrSchemaNotFound.WithCausef("schemaName", schemaName)
	}

	// check if exists
	table, exists := c.getTableLocked(schemaName, tableName)
	if exists {
		return table, true, nil
	}

	// create new schemasCache
	shardID, err := c.pickOneShardOnNode(nodeName)
	if err != nil {
		return nil, false, errors.WithMessagef(err, "cluster AllocTableID, clusterName:%s, schemaName:%s, tableName:%s, nodeName:%s", c.Name(), schemaName, tableName, nodeName)
	}

	// alloc table id
	tableID, err := c.allocTableID(ctx)
	if err != nil {
		return nil, false, errors.WithMessagef(err, "cluster AllocTableID, schemaName:%s, tableName:%s", schemaName, tableName)
	}

	// Save table in storage.
	tablePb := &clusterpb.Table{Id: tableID, Name: tableName, SchemaId: schema.GetID(), ShardId: shardID}
	tablePb, err = c.storage.CreateTable(ctx, c.clusterID, schema.GetID(), tablePb)
	if err != nil {
		return nil, false, errors.WithMessage(err, "cluster CreateTable")
	}

	// Update shardTopology in storage.
	shardTopologies, err := c.storage.ListShardTopologies(ctx, c.clusterID, []uint32{shardID})
	if err != nil {
		return nil, false, errors.WithMessage(err, "get or create table")
	}
	if len(shardTopologies) != 1 {
		return nil, false, ErrGetShardTopology.WithCausef("cluster CreateTable, shard has more than one shardTopology, shardID:%d, shardTopologies:%v",
			shardID, shardTopologies)
	}

	shardTopology := shardTopologies[0]
	shardTopology.TableIds = append(shardTopology.TableIds, tableID)
	if err = c.storage.PutShardTopology(ctx, c.clusterID, shardTopology.GetVersion(), shardTopology); err != nil {
		return nil, false, errors.WithMessage(err, "cluster CreateTable")
	}

	// Update tableCache in memory.
	table = c.updateTableCacheLocked(shardID, schema, tablePb)
	return table, false, nil
}

func (c *Cluster) loadClusterTopologyLocked(ctx context.Context) (map[uint32][]*clusterpb.Shard, []uint32, error) {
	clusterTopology, err := c.storage.GetClusterTopology(ctx, c.clusterID)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "cluster loadClusterTopologyLocked")
	}
	c.metaData.clusterTopology = clusterTopology

	if c.metaData.clusterTopology == nil {
		return nil, nil, ErrClusterTopologyNotFound.WithCausef("cluster:%v", c)
	}

	shardMap := make(map[uint32][]*clusterpb.Shard)
	for _, shard := range c.metaData.clusterTopology.ShardView {
		shardMap[shard.Id] = append(shardMap[shard.Id], shard)
	}

	shardIDs := make([]uint32, 0, len(shardMap))
	for id := range shardMap {
		shardIDs = append(shardIDs, id)
	}

	return shardMap, shardIDs, nil
}

func (c *Cluster) loadShardTopologyLocked(ctx context.Context, shardIDs []uint32) (map[uint32]*clusterpb.ShardTopology, error) {
	topologies, err := c.storage.ListShardTopologies(ctx, c.clusterID, shardIDs)
	if err != nil {
		return nil, errors.WithMessage(err, "cluster loadShardTopologyLocked")
	}
	shardTopologyMap := make(map[uint32]*clusterpb.ShardTopology, len(shardIDs))
	for i, topology := range topologies {
		shardTopologyMap[shardIDs[i]] = topology
	}
	return shardTopologyMap, nil
}

func (c *Cluster) loadSchemaLocked(ctx context.Context) (map[string]*clusterpb.Schema, error) {
	schemas, err := c.storage.ListSchemas(ctx, c.clusterID)
	if err != nil {
		return nil, errors.WithMessage(err, "cluster loadSchemaLocked")
	}
	schemaMap := make(map[string]*clusterpb.Schema, len(schemas))
	for _, schema := range schemas {
		schemaMap[schema.Name] = schema
	}
	return schemaMap, nil
}

func (c *Cluster) loadNodeLocked(ctx context.Context) (map[string]*clusterpb.Node, error) {
	nodes, err := c.storage.ListNodes(ctx, c.clusterID)
	if err != nil {
		return nil, errors.WithMessage(err, "cluster loadNodeLocked")
	}

	nameNodes := make(map[string]*clusterpb.Node, len(nodes))
	for _, node := range nodes {
		nameNodes[node.Name] = node
	}
	return nameNodes, nil
}

func (c *Cluster) loadTableLocked(ctx context.Context, schemas map[string]*clusterpb.Schema) (map[string]map[uint64]*clusterpb.Table, error) {
	tables := make(map[string]map[uint64]*clusterpb.Table)
	for _, schema := range schemas {
		tablePbs, err := c.storage.ListTables(ctx, c.clusterID, schema.Id)
		if err != nil {
			return nil, errors.WithMessage(err, "cluster loadTableLocked")
		}
		for _, table := range tablePbs {
			if t, ok := tables[schema.GetName()]; ok {
				t[table.GetId()] = table
			} else {
				tables[schema.GetName()] = map[uint64]*clusterpb.Table{table.GetId(): table}
			}
		}
	}
	return tables, nil
}

func (c *Cluster) getSchemaLocked(schemaName string) (*Schema, bool) {
	schema, ok := c.schemasCache[schemaName]
	return schema, ok
}

func (c *Cluster) getTableLocked(schemaName string, tableName string) (*Table, bool) {
	table, ok := c.schemasCache[schemaName].tableMap[tableName]
	return table, ok
}

func (c *Cluster) GetTable(ctx context.Context, schemaName, tableName string) (*Table, bool, error) {
	c.lock.RLock()
	schema, ok := c.schemasCache[schemaName]
	if !ok {
		c.lock.RUnlock()
		return nil, false, ErrSchemaNotFound.WithCausef("schemaName:%s", schemaName)
	}

	table, exists := schema.getTable(tableName)
	if exists {
		c.lock.RUnlock()
		return table, true, nil
	}
	c.lock.RUnlock()

	// Search Table in storage.
	tablePb, exists, err := c.storage.GetTable(ctx, c.clusterID, schema.GetID(), tableName)
	if err != nil {
		return nil, false, errors.WithMessage(err, "cluster GetTable")
	}
	if exists {
		c.lock.Lock()
		defer c.lock.Unlock()

		shardID, err := c.getTableShardIDLocked(tablePb.GetId())
		if err != nil {
			return nil, false, errors.WithMessage(err, "cluster GetTable")
		}
		table = c.updateTableCacheLocked(shardID, schema, tablePb)
		return table, true, nil
	}

	return nil, false, nil
}

// GetShardByID return immutable `Shard`.
func (c *Cluster) GetShardByID(id uint32) (*Shard, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	shard, ok := c.shardsCache[id]
	if !ok {
		return nil, ErrShardNotFound.WithCausef("cluster GetShardByID, shardID:%s", id)
	}
	return shard, nil
}

func (c *Cluster) GetShardIDs(nodeName string) ([]uint32, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	node, ok := c.nodesCache[nodeName]
	if !ok {
		return nil, ErrNodeNotFound.WithCausef("cluster GetShardIDs, nodeName:%s", nodeName)
	}
	return node.shardIDs, nil
}

func (c *Cluster) RegisterNode(ctx context.Context, nodeInfo *metaservicepb.NodeInfo) error {
	nodePb := &clusterpb.Node{NodeStats: &clusterpb.NodeStats{Lease: nodeInfo.GetLease()}, Name: nodeInfo.GetEndpoint()}
	nodePb1, err := c.storage.CreateOrUpdateNode(ctx, c.clusterID, nodePb)
	if err != nil {
		return errors.WithMessagef(err, "cluster RegisterNode, nodeName:%s", nodeInfo.GetEndpoint())
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	node, ok := c.nodesCache[nodeInfo.GetEndpoint()]
	if ok {
		node.meta = nodePb1
	} else {
		c.nodesCache[nodeInfo.GetEndpoint()] = &Node{meta: nodePb1}
	}
	return nil
}

func (c *Cluster) allocSchemaID(ctx context.Context) (uint32, error) {
	id, err := c.schemaIDAlloc.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessage(err, "cluster alloc schema id failed")
	}
	return uint32(id), nil
}

func (c *Cluster) allocTableID(ctx context.Context) (uint64, error) {
	id, err := c.tableIDAlloc.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessage(err, "alloc table id failed")
	}
	return id, nil
}

// nolint
func (c *Cluster) allocShardID(ctx context.Context) (uint32, error) {
	id, err := c.shardIDAlloc.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessage(err, "cluster alloc shard id failed")
	}
	return uint32(id), nil
}

func (c *Cluster) pickOneShardOnNode(nodeName string) (uint32, error) {
	if node, ok := c.nodesCache[nodeName]; ok {
		if len(node.shardIDs) == 0 {
			return 0, ErrNodeShardsIsEmpty.WithCausef("nodeName:%s", nodeName)
		}

		idx := rand.Int31n(int32(len(node.shardIDs))) // #nosec G404
		return node.shardIDs[idx], nil
	}
	return 0, ErrNodeNotFound.WithCausef("nodeName:%s", nodeName)
}

func (c *Cluster) RouteTables(_ context.Context, schemaName string, tableNames []string) (*RouteTablesResult, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	schema, ok := c.schemasCache[schemaName]
	if !ok {
		return nil, ErrSchemaNotFound.WithCausef("schemaName:%s", schemaName)
	}

	routeEntries := make(map[string]*RouteEntry, len(tableNames))
	for _, tableName := range tableNames {
		table, exists := schema.getTable(tableName)
		if exists {
			shard := c.shardsCache[table.GetShardID()]

			nodeShards := make([]*NodeShard, 0, len(shard.nodes))
			for i, node := range shard.nodes {
				nodeShards = append(nodeShards, &NodeShard{
					Endpoint: node.GetName(),
					ShardInfo: &ShardInfo{
						ID:   shard.meta[i].GetId(),
						Role: shard.meta[i].GetShardRole(),
					},
				})
			}

			routeEntries[tableName] = &RouteEntry{
				Table: &TableInfo{
					ID:         table.GetID(),
					Name:       table.GetName(),
					SchemaID:   table.GetSchemaID(),
					SchemaName: table.GetSchemaName(),
				},
				NodeShards: nodeShards,
			}
		}
	}

	return &RouteTablesResult{
		Version:      c.metaData.clusterTopology.Version,
		RouteEntries: routeEntries,
	}, nil
}

func (c *Cluster) GetNodes(_ context.Context) (*GetNodesResult, error) {
	nodeShards := make([]*NodeShard, 0, len(c.nodesCache))

	c.lock.RLock()
	defer c.lock.RUnlock()

	for nodeName, node := range c.nodesCache {
		for _, shardID := range node.shardIDs {
			shard, ok := c.shardsCache[shardID]
			if !ok {
				return nil, ErrShardNotFound.WithCausef("shardID:%d", shardID)
			}

			var shardRole clusterpb.ShardRole
			for i := range shard.nodes {
				if shard.nodes[i].GetName() == nodeName {
					shardRole = shard.meta[i].GetShardRole()
					break
				}
			}

			nodeShards = append(nodeShards, &NodeShard{
				Endpoint: nodeName,
				ShardInfo: &ShardInfo{
					ID:      shardID,
					Role:    shardRole,
					Version: shard.version,
				},
			})
		}
	}

	return &GetNodesResult{
		ClusterTopologyVersion: c.metaData.clusterTopology.GetVersion(),
		NodeShards:             nodeShards,
	}, nil
}

func (c *Cluster) GetClusterVersion() uint64 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.metaData.clusterTopology.Version
}

func (c *Cluster) GetClusterMinNodeCount() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.metaData.cluster.MinNodeCount
}

func (c *Cluster) GetClusterShardTotal() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.metaData.cluster.ShardTotal
}

func (c *Cluster) GetClusterState() clusterpb.ClusterTopology_ClusterState {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.metaData.clusterTopology.State
}

func (c *Cluster) UpdateClusterTopology(ctx context.Context, state clusterpb.ClusterTopology_ClusterState, shardView []*clusterpb.Shard) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	clusterTopology, err := c.storage.GetClusterTopology(ctx, c.GetClusterID())
	if err != nil {
		return errors.WithMessage(err, "UpdateClusterTopology failed")
	}
	clusterTopology.ShardView = shardView
	clusterTopology.State = state
	if err = c.storage.PutClusterTopology(ctx, c.clusterID, c.metaData.clusterTopology.Version, clusterTopology); err != nil {
		return err
	}
	c.metaData.clusterTopology = clusterTopology
	return nil
}

type ShardWithLock struct {
	shardID uint32
	lock    sync.Mutex
}

func (c *Cluster) getShardLock(shardID uint32) *ShardWithLock {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.shardLock[shardID]
	if !ok {
		c.shardLock[shardID] = &ShardWithLock{shardID: shardID, lock: sync.Mutex{}}
	}
	return c.shardLock[shardID]
}

func (c *Cluster) LockShardByID(shardID uint32) bool {
	shardLock := c.getShardLock(shardID)
	return shardLock.lock.TryLock()
}

func (c *Cluster) LockShardByIDWithRetry(shardID uint32, maxRetry int, waitInterval time.Duration) bool {
	if maxRetry <= 0 {
		return false
	}
	lockResult := c.LockShardByID(shardID)
	if !lockResult {
		time.Sleep(waitInterval)
		return c.LockShardByIDWithRetry(shardID, maxRetry-1, waitInterval)
	}
	return lockResult
}

func (c *Cluster) UnlockShardByID(shardID uint32) {
	shardLock := c.getShardLock(shardID)
	if shardLock == nil {
		return
	}
	shardLock.lock.Unlock()
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.shardLock, shardID)
}
