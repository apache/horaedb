// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"
	"math"
	"path"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
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

func (s *metaStorageImpl) GetCluster(ctx context.Context, clusterID uint32) (*metapb.Cluster, error) {
	key := makeClusterKey(clusterID)
	value, err := s.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "meta storage get cluster failed, clusterID:%d, key:%s", clusterID, key)
	}

	meta := &metapb.Cluster{}
	if err = proto.Unmarshal([]byte(value), meta); err != nil {
		return nil, ErrParseGetCluster.WithCausef("proto parse failed, clusterID:%d, err:%s", clusterID, err)
	}
	return meta, nil
}

func (s *metaStorageImpl) PutCluster(ctx context.Context, clusterID uint32, meta *metapb.Cluster) error {
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

func (s *metaStorageImpl) GetClusterTopology(ctx context.Context, clusterID uint32) (*metapb.ClusterTopology, error) {
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

	clusterMetaData := &metapb.ClusterTopology{}
	if err = proto.Unmarshal([]byte(value), clusterMetaData); err != nil {
		return nil, ErrParseGetClusterTopology.WithCausef("proto parse failed, clusterID:%d, err:%s", clusterID, err)
	}
	return clusterMetaData, nil
}

func (s *metaStorageImpl) PutClusterTopology(ctx context.Context, clusterID uint32, latestVersion uint32, clusterMetaData *metapb.ClusterTopology) error {
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

func (s *metaStorageImpl) ListSchemas(ctx context.Context, clusterID uint32) ([]*metapb.Schema, error) {
	schemas := make([]*metapb.Schema, 0)
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
			schema := &metapb.Schema{}
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

// TODO: operator in a batch
func (s *metaStorageImpl) PutSchemas(ctx context.Context, clusterID uint32, schemas []*metapb.Schema) error {
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

// TODO: operator in a batch
func (s *metaStorageImpl) ListTables(ctx context.Context, clusterID uint32, schemaID uint32, tableIDs []uint64) ([]*metapb.Table, error) {
	tables := make([]*metapb.Table, 0)
	for _, tableID := range tableIDs {
		key := makeTableKey(clusterID, schemaID, tableID)
		value, err := s.Get(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "meta storage list tables failed, clusterID:%d, schemaID:%d, tableID:%d, key:%s", clusterID, schemaID, tableID, key)
		}

		tableData := &metapb.Table{}
		if err = proto.Unmarshal([]byte(value), tableData); err != nil {
			return nil, ErrParseGetTables.WithCausef("proto parse failed, clusterID:%d, schemaID:%d, tableID:%d, err:%s", clusterID, schemaID, tableID, err)
		}
		tables = append(tables, tableData)
	}
	return tables, nil
}

// TODO: operator in a batch
func (s *metaStorageImpl) PutTables(ctx context.Context, clusterID uint32, schemaID uint32, tables []*metapb.Table) error {
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
func (s *metaStorageImpl) ListShardTopologies(ctx context.Context, clusterID uint32, shardIDs []uint32) ([]*metapb.ShardTopology, error) {
	shardTableInfo := make([]*metapb.ShardTopology, 0)

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

		shardTopology := &metapb.ShardTopology{}
		if err = proto.Unmarshal([]byte(value), shardTopology); err != nil {
			return nil, ErrParseGetShardTopology.WithCausef("proto parse failed, clusterID:%d, shardID:%d, err:%s", clusterID, shardID, err)
		}
		shardTableInfo = append(shardTableInfo, shardTopology)
	}
	return shardTableInfo, nil
}

// TODO: operator in a batch
func (s *metaStorageImpl) PutShardTopologies(ctx context.Context, clusterID uint32, shardIDs []uint32, latestVersion uint32, shardTableInfo []*metapb.ShardTopology) error {
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

func (s *metaStorageImpl) ListNodes(ctx context.Context, clusterID uint32) ([]*metapb.Node, error) {
	nodes := make([]*metapb.Node, 0)
	nextID := uint32(0)
	endKey := makeNodeKey(clusterID, math.MaxUint32)

	rangeLimit := s.opts.MaxScanLimit
	for {
		startKey := makeNodeKey(clusterID, nextID)
		_, res, err := s.Scan(ctx, startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= s.opts.MinScanLimit {
				continue
			}
			return nil, errors.Wrapf(err, "meta storage list nodes failed, clusterID:%d, start key:%s, end key:%s, range limit:%d", clusterID, startKey, endKey, rangeLimit)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		for _, r := range res {
			node := &metapb.Node{}
			if err := proto.Unmarshal([]byte(r), node); err != nil {
				return nil, ErrParseGetNodes.WithCausef("proto parse failed, clusterID:%d, err:%s", clusterID, err)
			}
			nodes = append(nodes, node)

			if node.GetId() == math.MaxUint32 {
				log.Warn("list node node_id has reached max value", zap.Uint32("node-id", node.GetId()))
				return nodes, nil
			}
			nextID = node.GetId() + 1
		}

		if len(res) < rangeLimit {
			return nodes, nil
		}
	}
}

// TODO: operator in a batch
func (s *metaStorageImpl) PutNodes(ctx context.Context, clusterID uint32, nodes []*metapb.Node) error {
	for _, node := range nodes {
		key := makeNodeKey(clusterID, node.Id)
		value, err := proto.Marshal(node)
		if err != nil {
			return ErrParsePutNodes.WithCausef("proto parse failed, clusterID:%d, nodeID:%d, err:%s", clusterID, node.Id, err)
		}

		if err = s.Put(ctx, key, string(value)); err != nil {
			return errors.Wrapf(err, "meta storage put nodes failed, clusterID:%d, nodeID:%d, key:%s", clusterID, node.Id, key)
		}
	}
	return nil
}
