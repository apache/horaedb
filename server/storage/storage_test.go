// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.
// TODO: add test cause in the future

package storage

import (
	"context"
	"path"
	"strconv"
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestCluster(t *testing.T) {
	re := require.New(t)
	s := NewStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	meta := &metapb.Cluster{Id: 1, Name: "name", MinNodeCount: 1, ReplicationFactor: 1, ShardTotal: 1}
	err := s.PutCluster(ctx, 0, meta)
	re.NoError(err)

	value, err := s.GetCluster(ctx, 0)
	re.NoError(err)
	re.Equal(meta.Id, value.Id)
	re.Equal(meta.Name, value.Name)
	re.Equal(meta.MinNodeCount, value.MinNodeCount)
	re.Equal(meta.ReplicationFactor, value.ReplicationFactor)
	re.Equal(meta.ShardTotal, value.ShardTotal)
}

func TestClusterTopology(t *testing.T) {
	re := require.New(t)
	s := NewStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	latestVersionKey := makeClusterTopologyLatestVersionKey(1)
	err := s.Put(ctx, latestVersionKey, fmtID(uint64(0)))
	re.NoError(err)

	clusterMetaData := &metapb.ClusterTopology{ClusterId: 1, DataVersion: 1, Cause: "cause", CreatedAt: 1}
	err = s.PutClusterTopology(ctx, 1, 0, clusterMetaData)
	re.NoError(err)

	value, err := s.GetClusterTopology(ctx, 1)
	re.NoError(err)
	re.Equal(clusterMetaData.ClusterId, value.ClusterId)
	re.Equal(clusterMetaData.DataVersion, value.DataVersion)
	re.Equal(clusterMetaData.Cause, value.Cause)
	re.Equal(clusterMetaData.CreatedAt, value.CreatedAt)
}

func TestSchemes(t *testing.T) {
	re := require.New(t)
	s := NewStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	schemas := make([]*metapb.Schema, 0)
	for i := 0; i < 10; i++ {
		schema := &metapb.Schema{Id: uint32(i), ClusterId: uint32(i), Name: "name"}
		schemas = append(schemas, schema)
	}

	err := s.PutSchemas(ctx, 0, schemas)
	re.NoError(err)

	value, err := s.ListSchemas(ctx, 0)
	re.NoError(err)
	for i := 0; i < 10; i++ {
		re.Equal(schemas[i].Id, value[i].Id)
		re.Equal(schemas[i].ClusterId, value[i].ClusterId)
		re.Equal(schemas[i].Name, value[i].Name)
	}
}

func TestTables(t *testing.T) {
	re := require.New(t)
	s := NewStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	tables := make([]*metapb.Table, 0)
	for i := 0; i < 10; i++ {
		table := &metapb.Table{Id: uint64(i), Name: "name", SchemaId: uint32(i), ShardId: uint32(i), Desc: "desc"}
		tables = append(tables, table)
	}

	err := s.PutTables(ctx, 0, 0, tables)
	re.NoError(err)

	tableID := make([]uint64, 0)
	for i := 0; i < 10; i++ {
		tableID = append(tableID, uint64(i))
	}

	value, err := s.ListTables(ctx, 0, 0, tableID)
	re.NoError(err)
	for i := 0; i < 10; i++ {
		re.Equal(tables[i].Id, value[i].Id)
		re.Equal(tables[i].SchemaId, value[i].SchemaId)
		re.Equal(tables[i].Name, value[i].Name)
		re.Equal(tables[i].ShardId, value[i].ShardId)
		re.Equal(tables[i].Desc, value[i].Desc)
	}

	err = s.DeleteTables(ctx, 0, 0, tableID)
	re.NoError(err)

	value, err = s.ListTables(ctx, 0, 0, tableID)
	re.NoError(err)
	for i := 0; i < 10; i++ {
		re.Equal(uint64(0), value[i].Id)
		re.Equal(uint32(0), value[i].SchemaId)
		re.Equal("", value[i].Name)
		re.Equal(uint32(0), value[i].ShardId)
		re.Equal("", value[i].Desc)
	}
}

func TestShardTopologies(t *testing.T) {
	re := require.New(t)
	s := NewStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	for i := 0; i < 10; i++ {
		latestVersionKey := makeShardLatestVersionKey(0, uint32(i))
		err := s.Put(ctx, latestVersionKey, fmtID(uint64(0)))
		re.NoError(err)
	}

	shardTableInfo := make([]*metapb.ShardTopology, 0)
	shardID := make([]uint32, 0)
	for i := 0; i < 10; i++ {
		shardTableData := &metapb.ShardTopology{Version: 1}
		shardTableInfo = append(shardTableInfo, shardTableData)
		shardID = append(shardID, uint32(i))
	}

	err := s.PutShardTopologies(ctx, 0, shardID, 0, shardTableInfo)
	re.NoError(err)

	value, err := s.ListShardTopologies(ctx, 0, shardID)
	re.NoError(err)
	for i := 0; i < 10; i++ {
		re.Equal(shardTableInfo[i].Version, value[i].Version)
	}
}

func TestNodes(t *testing.T) {
	re := require.New(t)
	s := NewStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	nodes := make([]*metapb.Node, 0)
	for i := 0; i < 10; i++ {
		node := &metapb.Node{Id: uint32(i), CreateTime: uint64(i), LastTouchTime: uint64(i)}
		nodes = append(nodes, node)
	}

	err := s.PutNodes(ctx, 0, nodes)
	re.NoError(err)

	value, err := s.ListNodes(ctx, 0)
	re.NoError(err)
	for i := 0; i < 10; i++ {
		re.Equal(nodes[i].Id, value[i].Id)
		re.Equal(nodes[i].CreateTime, value[i].CreateTime)
		re.Equal(nodes[i].LastTouchTime, value[i].LastTouchTime)
	}
}

func NewStorage(t *testing.T) Storage {
	cfg := etcdutil.NewTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)

	<-etcd.Server.ReadyNotify()

	endpoint := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint},
	})
	assert.NoError(t, err)

	rootPath := path.Join("/ceresmeta", strconv.FormatUint(100, 10))
	ops := Options{MaxScanLimit: 100, MinScanLimit: 10}

	return newEtcdStorage(client, rootPath, ops)
}
