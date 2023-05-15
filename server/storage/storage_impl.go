// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"
	"math"
	"strconv"
	"strings"

	"github.com/CeresDB/ceresdbproto/golang/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
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

func (s *metaStorageImpl) GetCluster(ctx context.Context, clusterID ClusterID) (Cluster, error) {
	clusterKey := makeClusterKey(s.rootPath, uint32(clusterID))

	value, err := etcdutil.Get(ctx, s.client, clusterKey)
	if err != nil {
		return Cluster{}, errors.WithMessagef(err, "get cluster, clusterID:%d, key:%s", clusterID, clusterKey)
	}

	clusterProto := &clusterpb.Cluster{}
	if err = proto.Unmarshal([]byte(value), clusterProto); err != nil {
		return Cluster{}, ErrDecode.WithCausef("decode cluster view, clusterID:%d, err:%v", clusterID, err)
	}

	return convertClusterPB(clusterProto), nil
}

func (s *metaStorageImpl) ListClusters(ctx context.Context) (ListClustersResult, error) {
	startKey := makeClusterKey(s.rootPath, 0)
	endKey := makeClusterKey(s.rootPath, math.MaxUint32)
	rangeLimit := s.opts.MaxScanLimit

	var clusters []Cluster
	do := func(key string, value []byte) error {
		cluster := &clusterpb.Cluster{}
		if err := proto.Unmarshal(value, cluster); err != nil {
			return ErrDecode.WithCausef("decode cluster, key:%s, value:%v, err:%v", key, value, err)
		}

		clusters = append(clusters, convertClusterPB(cluster))
		return nil
	}

	err := etcdutil.Scan(ctx, s.client, startKey, endKey, rangeLimit, do)
	if err != nil {
		return ListClustersResult{}, errors.WithMessagef(err, "etcd scan clusters, start key:%s, end key:%s, range limit:%d", startKey, endKey, rangeLimit)
	}

	return ListClustersResult{
		Clusters: clusters,
	}, nil
}

// CreateCluster return error if the cluster already exists.
func (s *metaStorageImpl) CreateCluster(ctx context.Context, req CreateClusterRequest) error {
	c := convertClusterToPB(req.Cluster)
	value, err := proto.Marshal(&c)
	if err != nil {
		return ErrEncode.WithCausef("encode cluster，clusterID:%d, err:%v", req.Cluster.ID, err)
	}

	key := makeClusterKey(s.rootPath, c.Id)

	// Check if the key exists, if not，create cluster; Otherwise, the cluster already exists and return an error.
	keyMissing := clientv3util.KeyMissing(key)
	opCreateCluster := clientv3.OpPut(key, string(value))

	resp, err := s.client.Txn(ctx).
		If(keyMissing).
		Then(opCreateCluster).
		Commit()
	if err != nil {
		return errors.WithMessagef(err, "create cluster, clusterID:%d, key:%s", req.Cluster.ID, key)
	}
	if !resp.Succeeded {
		return ErrCreateClusterAgain.WithCausef("cluster may already exist, clusterID:%d, key:%s, resp:%v", req.Cluster.ID, key, resp)
	}
	return nil
}

// UpdateCluster return an error if the cluster does not exist.
func (s *metaStorageImpl) UpdateCluster(ctx context.Context, req UpdateClusterRequest) error {
	c := convertClusterToPB(req.Cluster)
	value, err := proto.Marshal(&c)
	if err != nil {
		return ErrEncode.WithCausef("encode cluster，clusterID:%d, err:%v", req.Cluster.ID, err)
	}

	key := makeClusterKey(s.rootPath, c.Id)

	keyExists := clientv3util.KeyExists(key)
	opUpdateCluster := clientv3.OpPut(key, string(value))

	resp, err := s.client.Txn(ctx).
		If(keyExists).
		Then(opUpdateCluster).
		Commit()
	if err != nil {
		return errors.WithMessagef(err, "update cluster, clusterID:%d, key:%s", req.Cluster.ID, key)
	}
	if !resp.Succeeded {
		return ErrUpdateCluster.WithCausef("update cluster failed, clusterID:%d, key:%s, resp:%v", req.Cluster.ID, key, resp)
	}
	return nil
}

// CreateClusterView return error if the cluster view already exists.
func (s *metaStorageImpl) CreateClusterView(ctx context.Context, req CreateClusterViewRequest) error {
	clusterViewPB := convertClusterViewToPB(req.ClusterView)
	value, err := proto.Marshal(&clusterViewPB)
	if err != nil {
		return ErrEncode.WithCausef("encode cluster view, clusterID:%d, err:%v", clusterViewPB.ClusterId, err)
	}

	key := makeClusterViewKey(s.rootPath, clusterViewPB.ClusterId, fmtID(clusterViewPB.Version))
	latestVersionKey := makeClusterViewLatestVersionKey(s.rootPath, clusterViewPB.ClusterId)

	// Check if the key and latest version key exists, if not，create cluster view and latest version; Otherwise, the cluster view already exists and return an error.
	latestVersionKeyMissing := clientv3util.KeyMissing(latestVersionKey)
	keyMissing := clientv3util.KeyMissing(key)
	opCreateClusterTopology := clientv3.OpPut(key, string(value))
	opCreateClusterTopologyLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(clusterViewPB.Version))

	resp, err := s.client.Txn(ctx).
		If(latestVersionKeyMissing, keyMissing).
		Then(opCreateClusterTopology, opCreateClusterTopologyLatestVersion).
		Commit()
	if err != nil {
		return errors.WithMessagef(err, "create cluster view, clusterID:%d, key:%s", clusterViewPB.ClusterId, key)
	}
	if !resp.Succeeded {
		return ErrCreateClusterViewAgain.WithCausef("cluster view may already exist, clusterID:%d, key:%s, resp:%v", clusterViewPB.ClusterId, key, resp)
	}
	return nil
}

func (s *metaStorageImpl) GetClusterView(ctx context.Context, req GetClusterViewRequest) (GetClusterViewResult, error) {
	key := makeClusterViewLatestVersionKey(s.rootPath, uint32(req.ClusterID))
	version, err := etcdutil.Get(ctx, s.client, key)
	if err != nil {
		return GetClusterViewResult{}, errors.WithMessagef(err, "get cluster view latest version, clusterID:%d, key:%s", req.ClusterID, key)
	}

	key = makeClusterViewKey(s.rootPath, uint32(req.ClusterID), version)
	value, err := etcdutil.Get(ctx, s.client, key)
	if err != nil {
		return GetClusterViewResult{}, errors.WithMessagef(err, "get cluster view, clusterID:%d, key:%s", req.ClusterID, key)
	}

	clusterView := &clusterpb.ClusterView{}
	if err = proto.Unmarshal([]byte(value), clusterView); err != nil {
		return GetClusterViewResult{}, ErrDecode.WithCausef("decode cluster view, clusterID:%d, err:%v", req.ClusterID, err)
	}
	return GetClusterViewResult{
		ClusterView: convertClusterViewPB(clusterView),
	}, nil
}

func (s *metaStorageImpl) UpdateClusterView(ctx context.Context, req UpdateClusterViewRequest) error {
	clusterViewPB := convertClusterViewToPB(req.ClusterView)

	value, err := proto.Marshal(&clusterViewPB)
	if err != nil {
		return ErrEncode.WithCausef("encode cluster view, clusterID:%d, err:%v", req.ClusterID, err)
	}

	key := makeClusterViewKey(s.rootPath, uint32(req.ClusterID), fmtID(clusterViewPB.Version))
	latestVersionKey := makeClusterViewLatestVersionKey(s.rootPath, uint32(req.ClusterID))

	// Check whether the latest version is equal to that in etcd. If it is equal，update cluster view and latest version; Otherwise, return an error.
	latestVersionEquals := clientv3.Compare(clientv3.Value(latestVersionKey), "=", fmtID(req.LatestVersion))
	opPutClusterTopology := clientv3.OpPut(key, string(value))
	opPutLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(clusterViewPB.Version))

	resp, err := s.client.Txn(ctx).
		If(latestVersionEquals).
		Then(opPutClusterTopology, opPutLatestVersion).
		Commit()
	if err != nil {
		return errors.WithMessagef(err, "put cluster view, clusterID:%d, key:%s", req.ClusterID, key)
	}
	if !resp.Succeeded {
		return ErrUpdateClusterViewConflict.WithCausef("cluster view may have been modified, clusterID:%d, key:%s, resp:%v", req.ClusterID, key, resp)
	}

	return nil
}

func (s *metaStorageImpl) ListSchemas(ctx context.Context, req ListSchemasRequest) (ListSchemasResult, error) {
	startKey := makeSchemaKey(s.rootPath, uint32(req.ClusterID), 0)
	endKey := makeSchemaKey(s.rootPath, uint32(req.ClusterID), math.MaxUint32)
	rangeLimit := s.opts.MaxScanLimit

	var schemas []Schema
	do := func(key string, value []byte) error {
		schema := &clusterpb.Schema{}
		if err := proto.Unmarshal(value, schema); err != nil {
			return ErrDecode.WithCausef("decode schema, key:%s, value:%v, clusterID:%d, err:%v", key, value, req.ClusterID, err)
		}

		schemas = append(schemas, convertSchemaPB(schema))
		return nil
	}

	err := etcdutil.Scan(ctx, s.client, startKey, endKey, rangeLimit, do)
	if err != nil {
		return ListSchemasResult{}, errors.WithMessagef(err, "scan schemas, clusterID:%d, start key:%s, end key:%s, range limit:%d", req.ClusterID, startKey, endKey, rangeLimit)
	}

	return ListSchemasResult{Schemas: schemas}, nil
}

// CreateSchema return error if the schema already exists.
func (s *metaStorageImpl) CreateSchema(ctx context.Context, req CreateSchemaRequest) error {
	schema := convertSchemaToPB(req.Schema)
	value, err := proto.Marshal(&schema)
	if err != nil {
		return ErrDecode.WithCausef("encode schema, clusterID:%d, schemaID:%d, err:%v", req.ClusterID, schema.Id, err)
	}

	key := makeSchemaKey(s.rootPath, uint32(req.ClusterID), schema.Id)

	// Check if the key exists, if not，create schema; Otherwise, the schema already exists and return an error.
	keyMissing := clientv3util.KeyMissing(key)
	opCreateSchema := clientv3.OpPut(key, string(value))

	resp, err := s.client.Txn(ctx).
		If(keyMissing).
		Then(opCreateSchema).
		Commit()
	if err != nil {
		return errors.WithMessagef(err, "create schema, clusterID:%d, schemaID:%d, key:%s", req.ClusterID, schema.Id, key)
	}
	if !resp.Succeeded {
		return ErrCreateSchemaAgain.WithCausef("schema may already exist, clusterID:%d, schemaID:%d, key:%s, resp:%v", req.ClusterID, schema.Id, key, resp)
	}
	return nil
}

// CreateTable return error if the table already exists.
func (s *metaStorageImpl) CreateTable(ctx context.Context, req CreateTableRequest) error {
	table := convertTableToPB(req.Table)
	value, err := proto.Marshal(&table)
	if err != nil {
		return ErrEncode.WithCausef("encode table, clusterID:%d, schemaID:%d, tableID:%d, err:%v", req.ClusterID, req.Table.ID, table.Id, err)
	}

	key := makeTableKey(s.rootPath, uint32(req.ClusterID), uint32(req.SchemaID), table.Id)
	nameToIDKey := makeNameToIDKey(s.rootPath, uint32(req.ClusterID), uint32(req.SchemaID), table.Name)

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
		return errors.WithMessagef(err, "create table, clusterID:%d, schemaID:%d, tableID:%d, key:%s", req.ClusterID, req.SchemaID, table.Id, key)
	}
	if !resp.Succeeded {
		return ErrCreateTableAgain.WithCausef("table may already exist, clusterID:%d, schemaID:%d, tableID:%d, key:%s, resp:%v", req.ClusterID, req.SchemaID, table.Id, key, resp)
	}
	return nil
}

func (s *metaStorageImpl) GetTable(ctx context.Context, req GetTableRequest) (GetTableResult, error) {
	value, err := etcdutil.Get(ctx, s.client, makeNameToIDKey(s.rootPath, uint32(req.ClusterID), uint32(req.SchemaID), req.TableName))
	if err == etcdutil.ErrEtcdKVGetNotFound {
		return GetTableResult{
			Exists: false,
		}, nil
	}
	if err != nil {
		return GetTableResult{}, errors.WithMessagef(err, "get table id, clusterID:%d, schemaID:%d, table name:%s", req.ClusterID, req.SchemaID, req.TableName)
	}

	tableID, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return GetTableResult{}, errors.WithMessagef(err, "string to int failed")
	}

	key := makeTableKey(s.rootPath, uint32(req.ClusterID), uint32(req.SchemaID), tableID)
	value, err = etcdutil.Get(ctx, s.client, key)
	if err != nil {
		return GetTableResult{}, errors.WithMessagef(err, "get table, clusterID:%d, schemaID:%d, tableID:%d, key:%s", req.ClusterID, req.SchemaID, tableID, key)
	}

	table := &clusterpb.Table{}
	if err = proto.Unmarshal([]byte(value), table); err != nil {
		return GetTableResult{}, ErrDecode.WithCausef("decode table, clusterID:%d, schemaID:%d, tableID:%d, err:%v", req.ClusterID, req.SchemaID, tableID, err)
	}

	return GetTableResult{
		Table:  convertTablePB(table),
		Exists: true,
	}, nil
}

func (s *metaStorageImpl) ListTables(ctx context.Context, req ListTableRequest) (ListTablesResult, error) {
	startKey := makeTableKey(s.rootPath, uint32(req.ClusterID), uint32(req.SchemaID), 0)
	endKey := makeTableKey(s.rootPath, uint32(req.ClusterID), uint32(req.SchemaID), math.MaxUint64)
	rangeLimit := s.opts.MaxScanLimit

	var tables []Table
	do := func(key string, value []byte) error {
		tablePB := &clusterpb.Table{}
		if err := proto.Unmarshal(value, tablePB); err != nil {
			return ErrDecode.WithCausef("decode table, key:%s, value:%v, clusterID:%d, schemaID:%d, err:%v", key, value, req.ClusterID, req.SchemaID, err)
		}
		table := convertTablePB(tablePB)
		tables = append(tables, table)
		return nil
	}
	err := etcdutil.Scan(ctx, s.client, startKey, endKey, rangeLimit, do)
	if err != nil {
		return ListTablesResult{}, errors.WithMessagef(err, "scan tables, clusterID:%d, schemaID:%d, start key:%s, end key:%s, range limit:%d", req.ClusterID, req.SchemaID, startKey, endKey, rangeLimit)
	}

	return ListTablesResult{
		Tables: tables,
	}, nil
}

func (s *metaStorageImpl) DeleteTable(ctx context.Context, req DeleteTableRequest) error {
	nameKey := makeNameToIDKey(s.rootPath, uint32(req.ClusterID), uint32(req.SchemaID), req.TableName)

	value, err := etcdutil.Get(ctx, s.client, nameKey)
	if err != nil {
		return errors.WithMessagef(err, "get table id, clusterID:%d, schemaID:%d, table name:%s", req.ClusterID, req.SchemaID, req.TableName)
	}

	tableID, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return errors.WithMessagef(err, "string to int failed")
	}

	key := makeTableKey(s.rootPath, uint32(req.ClusterID), uint32(req.SchemaID), tableID)

	nameKeyExists := clientv3util.KeyExists(nameKey)
	idKeyExists := clientv3util.KeyExists(key)

	opDeleteNameToID := clientv3.OpDelete(nameKey)
	opDeleteTable := clientv3.OpDelete(key)

	resp, err := s.client.Txn(ctx).
		If(nameKeyExists, idKeyExists).
		Then(opDeleteNameToID, opDeleteTable).
		Commit()
	if err != nil {
		return errors.WithMessagef(err, "delete table, clusterID:%d, schemaID:%d, tableID:%d, tableName:%s", req.ClusterID, req.SchemaID, tableID, req.TableName)
	}
	if !resp.Succeeded {
		return ErrDeleteTableAgain.WithCausef("table may have been deleted, clusterID:%d, schemaID:%d, tableID:%d, tableName:%s", req.ClusterID, req.SchemaID, tableID, req.TableName)
	}

	return nil
}

func (s *metaStorageImpl) CreateShardViews(ctx context.Context, req CreateShardViewsRequest) error {
	keysMissing := make([]clientv3.Cmp, 0)
	opCreateShardTopologiesAndLatestVersion := make([]clientv3.Op, 0)

	for _, shardView := range req.ShardViews {
		shardViewPB := convertShardViewToPB(shardView)
		value, err := proto.Marshal(&shardViewPB)
		if err != nil {
			return ErrEncode.WithCausef("encode shard clusterView, clusterID:%d, shardID:%d, err:%v", req.ClusterID, shardView.ShardID, err)
		}

		key := makeShardViewKey(s.rootPath, uint32(req.ClusterID), uint32(shardView.ShardID), fmtID(shardView.Version))
		latestVersionKey := makeShardViewLatestVersionKey(s.rootPath, uint32(req.ClusterID), uint32(shardView.ShardID))

		// Check if the key and latest version key exists, if not，create shard clusterView and latest version; Otherwise, the shard clusterView already exists and return an error.
		keysMissing = append(keysMissing, clientv3util.KeyMissing(key), clientv3util.KeyMissing(latestVersionKey))
		opCreateShardTopologiesAndLatestVersion = append(opCreateShardTopologiesAndLatestVersion, clientv3.OpPut(key, string(value)), clientv3.OpPut(latestVersionKey, fmtID(shardView.Version)))
	}
	resp, err := s.client.Txn(ctx).
		If(keysMissing...).
		Then(opCreateShardTopologiesAndLatestVersion...).
		Commit()
	if err != nil {
		return errors.WithMessagef(err, "create shard view, clusterID:%d", req.ClusterID)
	}
	if !resp.Succeeded {
		return ErrCreateShardViewAgain.WithCausef("shard view may already exist, clusterID:%d, resp:%v", req.ClusterID, resp)
	}
	return nil
}

func (s *metaStorageImpl) ListShardViews(ctx context.Context, req ListShardViewsRequest) (ListShardViewsResult, error) {
	var shardViews []ShardView
	prefix := makeShardViewVersionKey(s.rootPath, uint32(req.ClusterID))
	keys, err := etcdutil.List(ctx, s.client, prefix)
	if err != nil {
		return ListShardViewsResult{}, errors.WithMessagef(err, "list shard view, clusterID:%d", req.ClusterID)
	}
	for _, key := range keys {
		if strings.HasSuffix(key, latestVersion) {
			shardIDKey, err := decodeShardViewVersionKey(key)
			if err != nil {
				return ListShardViewsResult{}, errors.WithMessagef(err, "list shard view latest version, clusterID:%d, shardIDKey:%s, key:%s", req.ClusterID, shardIDKey, key)
			}
			shardID, err := strconv.ParseUint(shardIDKey, 10, 32)
			if err != nil {
				return ListShardViewsResult{}, errors.WithMessagef(err, "list shard view latest version, clusterID:%d, shardID:%d, key:%s", req.ClusterID, shardID, key)
			}

			version, err := etcdutil.Get(ctx, s.client, key)
			if err != nil {
				return ListShardViewsResult{}, errors.WithMessagef(err, "list shard view latest version, clusterID:%d, shardID:%d, key:%s", req.ClusterID, shardID, key)
			}

			key = makeShardViewKey(s.rootPath, uint32(req.ClusterID), uint32(shardID), version)
			value, err := etcdutil.Get(ctx, s.client, key)
			if err != nil {
				return ListShardViewsResult{}, errors.WithMessagef(err, "list shard view, clusterID:%d, shardID:%d, key:%s", req.ClusterID, shardID, key)
			}

			shardViewPB := &clusterpb.ShardView{}
			if err = proto.Unmarshal([]byte(value), shardViewPB); err != nil {
				return ListShardViewsResult{}, ErrDecode.WithCausef("decode shard view, clusterID:%d, shardID:%d, err:%v", req.ClusterID, shardID, err)
			}
			shardView := convertShardViewPB(shardViewPB)
			shardViews = append(shardViews, shardView)
		}
	}
	return ListShardViewsResult{
		ShardViews: shardViews,
	}, nil
}

func (s *metaStorageImpl) UpdateShardView(ctx context.Context, req UpdateShardViewRequest) error {
	shardViewPB := convertShardViewToPB(req.ShardView)
	value, err := proto.Marshal(&shardViewPB)
	if err != nil {
		return ErrEncode.WithCausef("encode shard view, clusterID:%d, shardID:%d, err:%v", req.ClusterID, req.ShardView.ShardID, err)
	}

	key := makeShardViewKey(s.rootPath, uint32(req.ClusterID), shardViewPB.ShardId, fmtID(shardViewPB.GetVersion()))
	latestVersionKey := makeShardViewLatestVersionKey(s.rootPath, uint32(req.ClusterID), shardViewPB.ShardId)

	// Check whether the latest version is equal to that in etcd. If it is equal，update shard clusterView and latest version; Otherwise, return an error.
	latestVersionEquals := clientv3.Compare(clientv3.Value(latestVersionKey), "=", fmtID(req.LatestVersion))
	opPutLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(shardViewPB.Version))
	opPutShardTopology := clientv3.OpPut(key, string(value))

	resp, err := s.client.Txn(ctx).
		If(latestVersionEquals).
		Then(opPutLatestVersion, opPutShardTopology).
		Commit()
	if err != nil {
		return errors.WithMessagef(err, "fail to put shard clusterView, clusterID:%d, shardID:%d, key:%s", req.ClusterID, shardViewPB.ShardId, key)
	}
	if !resp.Succeeded {
		return ErrUpdateShardViewConflict.WithCausef("shard view may have been modified, clusterID:%d, shardID:%d, key:%s, resp:%v", req.ClusterID, shardViewPB.ShardId, key, resp)
	}

	return nil
}

func (s *metaStorageImpl) ListNodes(ctx context.Context, req ListNodesRequest) (ListNodesResult, error) {
	startKey := makeNodeKey(s.rootPath, uint32(req.ClusterID), string([]byte{0}))
	endKey := makeNodeKey(s.rootPath, uint32(req.ClusterID), string([]byte{255}))
	rangeLimit := s.opts.MaxScanLimit

	var nodes []Node
	do := func(key string, value []byte) error {
		nodePB := &clusterpb.Node{}
		if err := proto.Unmarshal(value, nodePB); err != nil {
			return ErrDecode.WithCausef("decode node, key:%s, value:%v, clusterID:%d, err:%v", key, value, req.ClusterID, err)
		}
		node := convertNodePB(nodePB)
		nodes = append(nodes, node)
		return nil
	}

	err := etcdutil.Scan(ctx, s.client, startKey, endKey, rangeLimit, do)
	if err != nil {
		return ListNodesResult{}, errors.WithMessagef(err, "scan nodes, clusterID:%d, start key:%s, end key:%s, range limit:%d", req.ClusterID, startKey, endKey, rangeLimit)
	}

	return ListNodesResult{
		Nodes: nodes,
	}, nil
}

func (s *metaStorageImpl) CreateOrUpdateNode(ctx context.Context, req CreateOrUpdateNodeRequest) error {
	nodePB := convertNodeToPB(req.Node)

	key := makeNodeKey(s.rootPath, uint32(req.ClusterID), req.Node.Name)

	value, err := proto.Marshal(&nodePB)
	if err != nil {
		return ErrEncode.WithCausef("encode node, clusterID:%d, node name:%s, err:%v", req.ClusterID, req.Node.Name, err)
	}

	_, err = s.client.Put(ctx, key, string(value))
	if err != nil {
		return errors.WithMessagef(err, "create or update node, clusterID:%d, node name:%s, key:%s", req.ClusterID, req.Node.Name, key)
	}

	return nil
}
