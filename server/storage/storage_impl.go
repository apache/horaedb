// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"
	"math"
	"path"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
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
	KV

	opts Options

	rootPath string
}

// NewMetaStorageImpl creates a new base storage endpoint with the given KV and encryption key manager.
// It should be embedded insIDe a storage backend.
func NewMetaStorageImpl(
	kv KV,
	opts Options,
	rootPath string,
) Storage {
	return &metaStorageImpl{kv, opts, rootPath}
}

// newEtcdBackend is used to create a new etcd backend.
func newEtcdStorage(client *clientv3.Client, rootPath string, opts Options) Storage {
	return NewMetaStorageImpl(
		NewEtcdKV(client, rootPath), opts, rootPath)
}

func (s *metaStorageImpl) ListClusters(ctx context.Context) ([]*clusterpb.Cluster, error) {
	return nil, nil
}

func (s *metaStorageImpl) CreateCluster(ctx context.Context, cluster *clusterpb.Cluster) (*clusterpb.Cluster, error) {
	return nil, nil
}

func (s *metaStorageImpl) GetCluster(ctx context.Context, clusterID uint32) (*clusterpb.Cluster, error) {
	key := makeClusterKey(clusterID)
	value, err := s.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "meta storage get cluster failed, clusterID:%d, key:%s", clusterID, key)
	}

	meta := &clusterpb.Cluster{}
	if err = proto.Unmarshal([]byte(value), meta); err != nil {
		return nil, ErrParseGetCluster.WithCausef("proto parse failed, clusterID:%d, err:%s", clusterID, err)
	}
	return meta, nil
}

func (s *metaStorageImpl) CreateClusterTopology(ctx context.Context, clusterTopology *clusterpb.ClusterTopology) error {
	return nil
}

func (s *metaStorageImpl) PutCluster(ctx context.Context, clusterID uint32, meta *clusterpb.Cluster) error {
	value, err := proto.Marshal(meta)
	if err != nil {
		return ErrParsePutCluster.WithCausef("proto parse failed, clusterID:%d, err:%s", clusterID, err)
	}

	key := makeClusterKey(clusterID)
	err = s.Put(ctx, key, string(value))
	if err != nil {
		return errors.Wrapf(err, "meta storage put cluster failed, clusterID:%d, key:%s", clusterID, key)
	}
	return nil
}

func (s *metaStorageImpl) GetClusterTopology(ctx context.Context, clusterID uint32) (*clusterpb.ClusterTopology, error) {
	key := makeClusterTopologyLatestVersionKey(clusterID)
	version, err := s.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "meta storage get cluster topology latest version failed, clusterID:%d, key:%s", clusterID, key)
	}

	key = makeClusterTopologyKey(clusterID, version)
	value, err := s.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "meta storage get cluster topology failed, clusterID:%d, key:%s", clusterID, key)
	}

	clusterMetaData := &clusterpb.ClusterTopology{}
	if err = proto.Unmarshal([]byte(value), clusterMetaData); err != nil {
		return nil, ErrParseGetClusterTopology.WithCausef("proto parse failed, clusterID:%d, err:%s", clusterID, err)
	}
	return clusterMetaData, nil
}

func (s *metaStorageImpl) PutClusterTopology(ctx context.Context, clusterID uint32, latestVersion uint32, clusterMetaData *clusterpb.ClusterTopology) error {
	value, err := proto.Marshal(clusterMetaData)
	if err != nil {
		return ErrParsePutClusterTopology.WithCausef("proto parse failed, clusterID:%d, err:%s", clusterID, err)
	}

	key := path.Join(s.rootPath, makeClusterTopologyKey(clusterID, fmtID(clusterMetaData.DataVersion)))
	latestVersionKey := path.Join(s.rootPath, makeClusterTopologyLatestVersionKey(clusterID))

	cmp := clientv3.Compare(clientv3.Value(latestVersionKey), "=", fmtID(uint64(latestVersion)))
	opPutClusterTopology := clientv3.OpPut(key, string(value))
	opPutLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(clusterMetaData.DataVersion))

	resp, err := s.Txn(ctx).
		If(cmp).
		Then(opPutClusterTopology, opPutLatestVersion).
		Commit()
	if err != nil {
		return errors.Wrapf(err, "meta storage put cluster topology failed, clusterID:%d, key:%s", clusterID, key)
	}
	if !resp.Succeeded {
		return ErrParsePutClusterTopology.WithCausef("clusterID:%d, resp:%s", clusterID, resp)
	}

	return nil
}

func (s *metaStorageImpl) ListSchemas(ctx context.Context, clusterID uint32) ([]*clusterpb.Schema, error) {
	schemas := make([]*clusterpb.Schema, 0)
	nextID := uint32(0)
	endKey := makeSchemaKey(clusterID, math.MaxUint32)

	rangeLimit := s.opts.MaxScanLimit
	for {
		startKey := makeSchemaKey(clusterID, nextID)
		_, res, err := s.Scan(ctx, startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= s.opts.MinScanLimit {
				continue
			}
			return nil, errors.Wrapf(err, "meta storage list schemas failed, clusterID:%d, start key:%s, end key:%s, range limit:%d", clusterID, startKey, endKey, rangeLimit)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		for _, r := range res {
			schema := &clusterpb.Schema{}
			if err := proto.Unmarshal([]byte(r), schema); err != nil {
				return nil, ErrParseGetSchemas.WithCausef("proto parse failed, clusterID:%d, err:%s", clusterID, err)
			}

			schemas = append(schemas, schema)
			if schema.GetId() == math.MaxUint32 {
				log.Warn("list schemas schema_id has reached max value", zap.Uint32("schema-id", schema.GetId()))
				return schemas, nil
			}
			nextID = schema.GetId() + 1
		}

		if len(res) < rangeLimit {
			return schemas, nil
		}
	}
}

func (s *metaStorageImpl) CreateSchema(ctx context.Context, clusterID uint32, schema *clusterpb.Schema) error {
	return nil
}

// TODO: operator in a batch
func (s *metaStorageImpl) PutSchemas(ctx context.Context, clusterID uint32, schemas []*clusterpb.Schema) error {
	for _, schema := range schemas {
		key := makeSchemaKey(clusterID, schema.Id)
		value, err := proto.Marshal(schema)
		if err != nil {
			return ErrParsePutSchemas.WithCausef("proto parse failed, clusterID:%d, schemaID:%d, err:%s", clusterID, schema.Id, err)
		}

		if err = s.Put(ctx, key, string(value)); err != nil {
			return errors.Wrapf(err, "meta storage put schemas failed, clusterID:%d, schemaID:%d, key:%s", clusterID, schema.Id, key)
		}
	}
	return nil
}

func (s *metaStorageImpl) CreateTable(ctx context.Context, clusterID uint32, schemaID uint32, table *clusterpb.Table) error {
	return nil
}

func (s *metaStorageImpl) GetTable(ctx context.Context, clusterID uint32, schemaID uint32, tableName string) (*clusterpb.Table, bool, error) {
	return nil, false, nil
}

func (s *metaStorageImpl) ListTables(ctx context.Context, clusterID uint32, schemaID uint32) ([]*clusterpb.Table, error) {
	return nil, nil
}

// TODO: operator in a batch
func (s *metaStorageImpl) PutTables(ctx context.Context, clusterID uint32, schemaID uint32, tables []*clusterpb.Table) error {
	for _, item := range tables {
		key := makeTableKey(clusterID, schemaID, item.Id)
		value, err := proto.Marshal(item)
		if err != nil {
			return ErrParsePutTables.WithCausef("proto parse failed, clusterID:%d, schemaID:%d, tableID:%d, err:%s", clusterID, schemaID, item.Id, err)
		}

		if err = s.Put(ctx, key, string(value)); err != nil {
			return errors.Wrapf(err, "meta storage put tables failed, clusterID:%d, schemaID:%d, tableID:%d, key:%s", clusterID, schemaID, item.Id, key)
		}
	}
	return nil
}

// TODO: operator in a batch
func (s *metaStorageImpl) DeleteTables(ctx context.Context, clusterID uint32, schemaID uint32, tableIDs []uint64) error {
	for _, tableID := range tableIDs {
		key := makeTableKey(clusterID, schemaID, tableID)
		if err := s.Delete(ctx, key); err != nil {
			return errors.Wrapf(err, "meta storage delete tables failed, clusterID:%d, schemaID:%d, tableID:%d, key:%s", clusterID, schemaID, tableID, key)
		}
	}
	return nil
}

// TODO: operator in a batch
func (s *metaStorageImpl) ListShardTopologies(ctx context.Context, clusterID uint32, shardIDs []uint32) ([]*clusterpb.ShardTopology, error) {
	shardTableInfo := make([]*clusterpb.ShardTopology, 0)

	for _, shardID := range shardIDs {
		key := makeShardLatestVersionKey(clusterID, shardID)
		version, err := s.Get(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "meta storage list shard topology latest version failed, clusterID:%d, shardID:%d, key:%s", clusterID, shardID, key)
		}

		key = makeShardKey(clusterID, shardID, version)
		value, err := s.Get(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "meta storage list shard topology failed, clusterID:%d, shardID:%d, key:%s", clusterID, shardID, key)
		}

		shardTopology := &clusterpb.ShardTopology{}
		if err = proto.Unmarshal([]byte(value), shardTopology); err != nil {
			return nil, ErrParseGetShardTopology.WithCausef("proto parse failed, clusterID:%d, shardID:%d, err:%s", clusterID, shardID, err)
		}
		shardTableInfo = append(shardTableInfo, shardTopology)
	}
	return shardTableInfo, nil
}

// TODO: operator in a batch
func (s *metaStorageImpl) PutShardTopologies(ctx context.Context, clusterID uint32, shardIDs []uint32, latestVersion uint32, shardTableInfo []*clusterpb.ShardTopology) error {
	for index, shardID := range shardIDs {
		value, err := proto.Marshal(shardTableInfo[index])
		if err != nil {
			return ErrParsePutShardTopology.WithCausef("proto parse failed, clusterID:%d, shardID:%d, err:%s", clusterID, shardID, err)
		}

		key := path.Join(s.rootPath, makeShardKey(clusterID, shardID, fmtID(shardTableInfo[index].Version)))
		latestVersionKey := path.Join(s.rootPath, makeShardLatestVersionKey(clusterID, shardID))

		cmp := clientv3.Compare(clientv3.Value(latestVersionKey), "=", fmtID(uint64(latestVersion)))
		opPutLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(shardTableInfo[index].Version))
		opPutShardTopology := clientv3.OpPut(key, string(value))

		resp, err := s.Txn(ctx).
			If(cmp).
			Then(opPutLatestVersion, opPutShardTopology).
			Commit()
		if err != nil {
			return errors.Wrapf(err, "meta storage put shard failed, clusterID:%d, shardID:%d, key:%s", clusterID, shardID, key)
		}
		if !resp.Succeeded {
			return ErrParsePutShardTopology.WithCausef("clusterID:%d, shardID:%d, resp:%s", clusterID, shardID, resp)
		}
	}
	return nil
}

func (s *metaStorageImpl) ListNodes(ctx context.Context, clusterID uint32) ([]*clusterpb.Node, error) {
	return nil, nil
}

func (s *metaStorageImpl) PutNodes(ctx context.Context, clusterID uint32, nodes []*clusterpb.Node) error {
	return nil
}

func (s *metaStorageImpl) CreateOrUpdateNode(ctx context.Context, clusterID uint32, node *clusterpb.Node) (*clusterpb.Node, error) {
	return nil, nil
}
