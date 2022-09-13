// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/CeresDB/ceresmeta/server/etcdutil"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"
	"google.golang.org/protobuf/proto"
)

type Options struct {
	// MaxScanLimit is the max limit of the number of keys in a scan.
	MaxScanLimit int
	// MinScanLimit is the min limit of the number of keys in a scan.
	MinScanLimit int
}

// metaStorageImpl is the base underlying storage endpoint for all other upper
// specific storage backends. It should define some common storage interfaces and operations,
// which provIDes the default implementations for all kinds of storages.
type metaStorageImpl struct {
	client *clientv3.Client

	opts Options

	rootPath string
}

// newEtcdBackend is used to create a new etcd backend.
func newEtcdStorage(client *clientv3.Client, rootPath string, opts Options) Storage {
	return &metaStorageImpl{client, opts, rootPath}
}

func (s *metaStorageImpl) ListClusters(ctx context.Context) ([]*clusterpb.Cluster, error) {
	startKey := makeClusterKey(s.rootPath, 0)
	endKey := makeClusterKey(s.rootPath, math.MaxUint32)
	rangeLimit := s.opts.MaxScanLimit

	clusters := make([]*clusterpb.Cluster, 0)
	encode := func(value []byte) error {
		cluster := &clusterpb.Cluster{}
		if err := proto.Unmarshal(value, cluster); err != nil {
			return ErrEncodeCluster.WithCausef("fail to encode cluster, err:%v", err)
		}

		clusters = append(clusters, cluster)
		return nil
	}

	err := etcdutil.Scan(ctx, s.client, startKey, endKey, rangeLimit, encode)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to list clusters, start key:%s, end key:%s, range limit:%d", startKey, endKey, rangeLimit)
	}

	return clusters, nil
}

// Return error if the cluster already exists.
func (s *metaStorageImpl) CreateCluster(ctx context.Context, cluster *clusterpb.Cluster) (*clusterpb.Cluster, error) {
	now := time.Now()
	cluster.CreatedAt = uint64(now.Unix())

	value, err := proto.Marshal(cluster)
	if err != nil {
		return nil, ErrDecodeCluster.WithCausef("fail to decode cluster，clusterID:%d, err:%v", cluster.Id, err)
	}

	key := makeClusterKey(s.rootPath, cluster.Id)

	// Check if the key exists, if not，create cluster; Otherwise, the cluster already exists and return an error.
	keyMissing := clientv3util.KeyMissing(key)
	opCreateCluster := clientv3.OpPut(key, string(value))

	resp, err := s.client.Txn(ctx).
		If(keyMissing).
		Then(opCreateCluster).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create cluster, clusterID:%d, key:%s", cluster.Id, key)
	}
	if !resp.Succeeded {
		return nil, ErrCreateClusterAgain.WithCausef("cluster may already exist, clusterID:%d, key:%s, resp:%v", cluster.Id, key, resp)
	}
	return cluster, nil
}

// Return error if the cluster topology already exists.
func (s *metaStorageImpl) CreateClusterTopology(ctx context.Context, clusterTopology *clusterpb.ClusterTopology) (*clusterpb.ClusterTopology, error) {
	now := time.Now()
	clusterTopology.CreatedAt = uint64(now.Unix())

	value, err := proto.Marshal(clusterTopology)
	if err != nil {
		return nil, ErrDecodeClusterTopology.WithCausef("fail to decode cluster topology, clusterID:%d, err:%v", clusterTopology.ClusterId, err)
	}

	key := makeClusterTopologyKey(s.rootPath, clusterTopology.ClusterId, fmtID(clusterTopology.Version))
	latestVersionKey := makeClusterTopologyLatestVersionKey(s.rootPath, clusterTopology.ClusterId)

	// Check if the key and latest version key exists, if not，create cluster topology and latest version; Otherwise, the cluster topology already exists and return an error.
	latestVersionKeyMissing := clientv3util.KeyMissing(latestVersionKey)
	keyMissing := clientv3util.KeyMissing(key)
	opCreateClusterTopology := clientv3.OpPut(key, string(value))
	opCreateClusterTopologyLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(clusterTopology.Version))

	resp, err := s.client.Txn(ctx).
		If(latestVersionKeyMissing, keyMissing).
		Then(opCreateClusterTopology, opCreateClusterTopologyLatestVersion).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create cluster topology, clusterID:%d, key:%s", clusterTopology.ClusterId, key)
	}
	if !resp.Succeeded {
		return nil, ErrCreateClusterTopologyAgain.WithCausef("cluster topology may already exist, clusterID:%d, key:%s, resp:%v", clusterTopology.ClusterId, key, resp)
	}
	return clusterTopology, nil
}

func (s *metaStorageImpl) GetClusterTopology(ctx context.Context, clusterID uint32) (*clusterpb.ClusterTopology, error) {
	key := makeClusterTopologyLatestVersionKey(s.rootPath, clusterID)
	version, err := etcdutil.Get(ctx, s.client, key)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to get cluster topology latest version, clusterID:%d, key:%s", clusterID, key)
	}

	key = makeClusterTopologyKey(s.rootPath, clusterID, version)
	value, err := etcdutil.Get(ctx, s.client, key)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to get cluster topology, clusterID:%d, key:%s", clusterID, key)
	}

	clusterTopology := &clusterpb.ClusterTopology{}
	if err = proto.Unmarshal([]byte(value), clusterTopology); err != nil {
		return nil, ErrEncodeClusterTopology.WithCausef("fail to encode cluster topology, clusterID:%d, err:%v", clusterID, err)
	}
	return clusterTopology, nil
}

func (s *metaStorageImpl) PutClusterTopology(ctx context.Context, clusterID uint32, latestVersion uint64, clusterTopology *clusterpb.ClusterTopology) error {
	value, err := proto.Marshal(clusterTopology)
	if err != nil {
		return ErrDecodeClusterTopology.WithCausef("fail to decode cluster topology, clusterID:%d, err:%v", clusterID, err)
	}

	key := makeClusterTopologyKey(s.rootPath, clusterID, fmtID(clusterTopology.Version))
	latestVersionKey := makeClusterTopologyLatestVersionKey(s.rootPath, clusterID)

	// Check whether the latest version is equal to that in etcd. If it is equal，update cluster topology and latest version; Otherwise, return an error.
	latestVersionEquals := clientv3.Compare(clientv3.Value(latestVersionKey), "=", fmtID(latestVersion))
	opPutClusterTopology := clientv3.OpPut(key, string(value))
	opPutLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(clusterTopology.Version))

	resp, err := s.client.Txn(ctx).
		If(latestVersionEquals).
		Then(opPutClusterTopology, opPutLatestVersion).
		Commit()
	if err != nil {
		return errors.Wrapf(err, "fail to put cluster topology, clusterID:%d, key:%s", clusterID, key)
	}
	if !resp.Succeeded {
		return ErrPutClusterTopologyConflict.WithCausef("cluster topology may have been modified, clusterID:%d, key:%s, resp:%v", clusterID, key, resp)
	}

	return nil
}

func (s *metaStorageImpl) ListSchemas(ctx context.Context, clusterID uint32) ([]*clusterpb.Schema, error) {
	startKey := makeSchemaKey(s.rootPath, clusterID, 0)
	endKey := makeSchemaKey(s.rootPath, clusterID, math.MaxUint32)
	rangeLimit := s.opts.MaxScanLimit

	schemas := make([]*clusterpb.Schema, 0)
	encode := func(value []byte) error {
		schema := &clusterpb.Schema{}
		if err := proto.Unmarshal(value, schema); err != nil {
			return ErrEncodeSchema.WithCausef("fail to encode schema, clusterID:%d, err:%v", clusterID, err)
		}

		schemas = append(schemas, schema)
		return nil
	}

	err := etcdutil.Scan(ctx, s.client, startKey, endKey, rangeLimit, encode)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to list schemas, clusterID:%d, start key:%s, end key:%s, range limit:%d", clusterID, startKey, endKey, rangeLimit)
	}

	return schemas, nil
}

// Return error if the schema already exists.
func (s *metaStorageImpl) CreateSchema(ctx context.Context, clusterID uint32, schema *clusterpb.Schema) (*clusterpb.Schema, error) {
	now := time.Now()
	schema.CreatedAt = uint64(now.Unix())

	value, err := proto.Marshal(schema)
	if err != nil {
		return nil, ErrDecodeSchema.WithCausef("fail to decode schema, clusterID:%d, schemaID:%d, err:%v", clusterID, schema.Id, err)
	}

	key := makeSchemaKey(s.rootPath, clusterID, schema.Id)

	// Check if the key exists, if not，create schema; Otherwise, the schema already exists and return an error.
	keyMissing := clientv3util.KeyMissing(key)
	opCreateSchema := clientv3.OpPut(key, string(value))

	resp, err := s.client.Txn(ctx).
		If(keyMissing).
		Then(opCreateSchema).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create schema, clusterID:%d, schemaID:%d, key:%s", clusterID, schema.Id, key)
	}
	if !resp.Succeeded {
		return nil, ErrCreateSchemaAgain.WithCausef("schema may already exist, clusterID:%d, schemaID:%d, key:%s, resp:%v", clusterID, schema.Id, key, resp)
	}
	return schema, nil
}

// Return error if the table already exists.
func (s *metaStorageImpl) CreateTable(ctx context.Context, clusterID uint32, schemaID uint32, table *clusterpb.Table) (*clusterpb.Table, error) {
	now := time.Now()
	table.CreatedAt = uint64(now.Unix())

	value, err := proto.Marshal(table)
	if err != nil {
		return nil, ErrDecodeTable.WithCausef("fail to decode table, clusterID:%d, schemaID:%d, tableID:%d, err:%v", clusterID, schemaID, table.Id, err)
	}

	key := makeTableKey(s.rootPath, clusterID, schemaID, table.Id)
	nameToIDKey := makeNameToIDKey(s.rootPath, clusterID, schemaID, table.Name)

	// Check if the key and the name to id key exists, if not，create table; Otherwise, the table already exists and return an error.
	idKeyMissing := clientv3util.KeyMissing(key)
	nameKeyMissing := clientv3util.KeyMissing(nameToIDKey)
	opCreateTable := clientv3.OpPut(key, string(value))
	opCreateNameToID := clientv3.OpPut(nameToIDKey, fmtID(table.Id))

	resp, err := s.client.Txn(ctx).
		If(nameKeyMissing, idKeyMissing).
		Then(opCreateTable, opCreateNameToID).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create table, clusterID:%d, schemaID:%d, tableID:%d, key:%s", clusterID, schemaID, table.Id, key)
	}
	if !resp.Succeeded {
		return nil, ErrCreateTableAgain.WithCausef("table may already exist, clusterID:%d, schemaID:%d, tableID:%d, key:%s, resp:%v", clusterID, schemaID, table.Id, key, resp)
	}
	return table, nil
}

func (s *metaStorageImpl) GetTable(ctx context.Context, clusterID uint32, schemaID uint32, tableName string) (*clusterpb.Table, bool, error) {
	value, err := etcdutil.Get(ctx, s.client, makeNameToIDKey(s.rootPath, clusterID, schemaID, tableName))
	if err != nil {
		return nil, false, errors.Wrapf(err, "fail to get table id, clusterID:%d, schemaID:%d, table name:%s", clusterID, schemaID, tableName)
	}

	tableID, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, false, errors.Wrapf(err, "string to int failed")
	}

	key := makeTableKey(s.rootPath, clusterID, schemaID, tableID)
	value, err = etcdutil.Get(ctx, s.client, key)
	if err != nil {
		return nil, false, errors.Wrapf(err, "fail to get table, clusterID:%d, schemaID:%d, tableID:%d, key:%s", clusterID, schemaID, tableID, key)
	}

	table := &clusterpb.Table{}
	if err = proto.Unmarshal([]byte(value), table); err != nil {
		return nil, false, ErrEncodeTable.WithCausef("fail to encode table, clusterID:%d, schemaID:%d, tableID:%d, err:%v", clusterID, schemaID, tableID, err)
	}

	return table, true, nil
}

func (s *metaStorageImpl) ListTables(ctx context.Context, clusterID uint32, schemaID uint32) ([]*clusterpb.Table, error) {
	startKey := makeTableKey(s.rootPath, clusterID, schemaID, 0)
	endKey := makeTableKey(s.rootPath, clusterID, schemaID, math.MaxUint64)
	rangeLimit := s.opts.MaxScanLimit

	tables := make([]*clusterpb.Table, 0)
	encode := func(value []byte) error {
		table := &clusterpb.Table{}
		if err := proto.Unmarshal(value, table); err != nil {
			return ErrEncodeTable.WithCausef("fail to encode table, clusterID:%d, schemaID:%d, err:%v", clusterID, schemaID, err)
		}

		tables = append(tables, table)
		return nil
	}
	err := etcdutil.Scan(ctx, s.client, startKey, endKey, rangeLimit, encode)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to list tables, clusterID:%d, schemaID:%d, start key:%s, end key:%s, range limit:%d", clusterID, schemaID, startKey, endKey, rangeLimit)
	}

	return tables, nil
}

func (s *metaStorageImpl) DeleteTable(ctx context.Context, clusterID uint32, schemaID uint32, tableName string) error {
	nameKey := makeNameToIDKey(s.rootPath, clusterID, schemaID, tableName)

	value, err := etcdutil.Get(ctx, s.client, nameKey)
	if err != nil {
		return errors.Wrapf(err, "fail to get table id, clusterID:%d, schemaID:%d, table name:%s", clusterID, schemaID, tableName)
	}

	tableID, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return errors.Wrapf(err, "string to int failed")
	}

	key := makeTableKey(s.rootPath, clusterID, schemaID, tableID)

	nameKeyExists := clientv3util.KeyExists(nameKey)
	idKeyExists := clientv3util.KeyExists(key)

	opDeleteNameToID := clientv3.OpDelete(nameKey)
	opDeleteTable := clientv3.OpDelete(key)

	resp, err := s.client.Txn(ctx).
		If(nameKeyExists, idKeyExists).
		Then(opDeleteNameToID, opDeleteTable).
		Commit()
	if err != nil {
		return errors.Wrapf(err, "fail to delete table, clusterID:%d, schemaID:%d, tableID:%d, tableName:%s", clusterID, schemaID, tableID, tableName)
	}
	if !resp.Succeeded {
		return ErrDeleteTableAgain.WithCausef("table may already delete, clusterID:%d, schemaID:%d, tableID:%d, tableName:%s", clusterID, schemaID, tableID, tableName)
	}

	return nil
}

// Return error if the shard topologies already exists.
func (s *metaStorageImpl) CreateShardTopologies(ctx context.Context, clusterID uint32, shardTopologies []*clusterpb.ShardTopology) ([]*clusterpb.ShardTopology, error) {
	now := time.Now()

	keysMissing := make([]clientv3.Cmp, 0)
	opCreateShardTopologiesAndLatestVersion := make([]clientv3.Op, 0)

	for _, shardTopology := range shardTopologies {
		shardTopology.CreatedAt = uint64(now.Unix())

		value, err := proto.Marshal(shardTopology)
		if err != nil {
			return nil, ErrDecodeShardTopology.WithCausef("fail to decode shard topology, clusterID:%d, shardID:%d, err:%v", clusterID, shardTopology.ShardId, err)
		}

		key := makeShardTopologyKey(s.rootPath, clusterID, shardTopology.GetShardId(), fmtID(shardTopology.Version))
		latestVersionKey := makeShardLatestVersionKey(s.rootPath, clusterID, shardTopology.GetShardId())

		// Check if the key and latest version key exists, if not，create shard topology and latest version; Otherwise, the shard topology already exists and return an error.
		keysMissing = append(keysMissing, clientv3util.KeyMissing(key), clientv3util.KeyMissing(latestVersionKey))
		opCreateShardTopologiesAndLatestVersion = append(opCreateShardTopologiesAndLatestVersion, clientv3.OpPut(key, string(value)), clientv3.OpPut(latestVersionKey, fmtID(shardTopology.Version)))
	}
	resp, err := s.client.Txn(ctx).
		If(keysMissing...).
		Then(opCreateShardTopologiesAndLatestVersion...).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create shard topology, clusterID:%d", clusterID)
	}
	if !resp.Succeeded {
		return nil, ErrCreateShardTopologyAgain.WithCausef("shard topology may already exist, clusterID:%d, resp:%v", clusterID, resp)
	}
	return shardTopologies, nil
}

func (s *metaStorageImpl) ListShardTopologies(ctx context.Context, clusterID uint32, shardIDs []uint32) ([]*clusterpb.ShardTopology, error) {
	shardTopologies := make([]*clusterpb.ShardTopology, 0)

	for _, shardID := range shardIDs {
		key := makeShardLatestVersionKey(s.rootPath, clusterID, shardID)
		version, err := etcdutil.Get(ctx, s.client, key)
		if err != nil {
			return nil, errors.Wrapf(err, "fail to list shard topology latest version, clusterID:%d, shardID:%d, key:%s", clusterID, shardID, key)
		}

		key = makeShardTopologyKey(s.rootPath, clusterID, shardID, version)
		value, err := etcdutil.Get(ctx, s.client, key)
		if err != nil {
			return nil, errors.Wrapf(err, "fail to list shard topology, clusterID:%d, shardID:%d, key:%s", clusterID, shardID, key)
		}

		shardTopology := &clusterpb.ShardTopology{}
		if err = proto.Unmarshal([]byte(value), shardTopology); err != nil {
			return nil, ErrEncodeShardTopology.WithCausef("fail to encode shard topology, clusterID:%d, shardID:%d, err:%v", clusterID, shardID, err)
		}
		shardTopologies = append(shardTopologies, shardTopology)
	}
	return shardTopologies, nil
}

func (s *metaStorageImpl) PutShardTopology(ctx context.Context, clusterID uint32, latestVersion uint64, shardTopology *clusterpb.ShardTopology) error {
	value, err := proto.Marshal(shardTopology)
	if err != nil {
		return ErrDecodeShardTopology.WithCausef("fail to decode shard topology, clusterID:%d, shardID:%d, err:%v", clusterID, shardTopology.ShardId, err)
	}

	key := makeShardTopologyKey(s.rootPath, clusterID, shardTopology.ShardId, fmtID(shardTopology.Version))
	latestVersionKey := makeShardLatestVersionKey(s.rootPath, clusterID, shardTopology.ShardId)

	// Check whether the latest version is equal to that in etcd. If it is equal，update shard topology and latest version; Otherwise, return an error.
	latestVersionEquals := clientv3.Compare(clientv3.Value(latestVersionKey), "=", fmtID(latestVersion))
	opPutLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(shardTopology.Version))
	opPutShardTopology := clientv3.OpPut(key, string(value))

	resp, err := s.client.Txn(ctx).
		If(latestVersionEquals).
		Then(opPutLatestVersion, opPutShardTopology).
		Commit()
	if err != nil {
		return errors.Wrapf(err, "fail to put shard topology, clusterID:%d, shardID:%d, key:%s", clusterID, shardTopology.ShardId, key)
	}
	if !resp.Succeeded {
		return ErrPutShardTopologyConflict.WithCausef("shard topology may have been modified, clusterID:%d, shardID:%d, key:%s, resp:%v", clusterID, shardTopology.ShardId, key, resp)
	}

	return nil
}

func (s *metaStorageImpl) ListNodes(ctx context.Context, clusterID uint32) ([]*clusterpb.Node, error) {
	startKey := makeNodeKey(s.rootPath, clusterID, string([]byte{0}))
	endKey := makeNodeKey(s.rootPath, clusterID, string([]byte{255}))
	rangeLimit := s.opts.MaxScanLimit

	nodes := make([]*clusterpb.Node, 0)
	encode := func(value []byte) error {
		node := &clusterpb.Node{}
		if err := proto.Unmarshal(value, node); err != nil {
			return ErrEncodeNode.WithCausef("fail to encode node, clusterID:%d, err:%v", clusterID, err)
		}

		nodes = append(nodes, node)
		return nil
	}

	err := etcdutil.Scan(ctx, s.client, startKey, endKey, rangeLimit, encode)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to list nodes, clusterID:%d, start key:%s, end key:%s, range limit:%d", clusterID, startKey, endKey, rangeLimit)
	}

	return nodes, nil
}

func (s *metaStorageImpl) CreateOrUpdateNode(ctx context.Context, clusterID uint32, node *clusterpb.Node) (*clusterpb.Node, error) {
	now := time.Now()
	node.LastTouchTime = uint64(now.Unix())

	CreateNode := node
	CreateNode.CreateTime = CreateNode.LastTouchTime
	UpdateNode := node

	key := makeNodeKey(s.rootPath, clusterID, node.Name)

	CreateNodeValue, err := proto.Marshal(CreateNode)
	if err != nil {
		return nil, ErrDecodeNode.WithCausef("fail to decode node, clusterID:%d, node name:%s, err:%v", clusterID, node.Name, err)
	}

	UpdateNodeValue, err := proto.Marshal(UpdateNode)
	if err != nil {
		return nil, ErrDecodeNode.WithCausef("fail to decode node, clusterID:%d, node name:%s, err:%v", clusterID, node.Name, err)
	}

	// Check if the key exists, if not，create node; Otherwise, update node.
	KeyMissing := clientv3util.KeyMissing(key)
	opCreateNode := clientv3.OpPut(key, string(CreateNodeValue))
	opUpdateNode := clientv3.OpPut(key, string(UpdateNodeValue))

	_, err = s.client.Txn(ctx).
		If(KeyMissing).
		Then(opCreateNode).
		Else(opUpdateNode).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create or update node, clusterID:%d, node name:%s, key:%s", clusterID, node.Name, key)
	}

	return node, nil
}
