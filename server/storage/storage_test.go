// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const (
	defaultRootPath       = "/ceresmeta"
	name0                 = "name0"
	nameFormat            = "name%d"
	defaultClusterID      = 1
	defaultSchemaID       = 1
	defaultVersion        = 0
	defaultCount          = 10
	defaultRequestTimeout = time.Second * 100
)

func TestStorage_CreateAndListCluster(t *testing.T) {
	re := require.New(t)
	s := newTestStorage(t)
	ctx := context.Background()

	// Test to create expectClusters.
	expectClusters := make([]Cluster, 0, defaultCount)
	for i := 0; i < defaultCount; i++ {
		cluster := Cluster{
			ID:           ClusterID(i),
			Name:         fmt.Sprintf(nameFormat, i),
			MinNodeCount: uint32(i),
			ShardTotal:   uint32(i),
			CreatedAt:    uint64(time.Now().UnixMilli()),
		}
		req := CreateClusterRequest{
			Cluster: cluster,
		}

		err := s.CreateCluster(ctx, req)
		re.NoError(err)
		expectClusters = append(expectClusters, cluster)
	}

	// Test to list expectClusters.
	ret, err := s.ListClusters(ctx)
	re.NoError(err)

	clusters := ret.Clusters
	for i := 0; i < defaultCount; i++ {
		re.Equal(expectClusters[i].ID, clusters[i].ID)
		re.Equal(expectClusters[i].Name, clusters[i].Name)
		re.Equal(expectClusters[i].MinNodeCount, clusters[i].MinNodeCount)
		re.Equal(expectClusters[i].CreatedAt, clusters[i].CreatedAt)
		re.Equal(expectClusters[i].ShardTotal, clusters[i].ShardTotal)
	}
}

func TestStorage_CreateAndGetClusterView(t *testing.T) {
	re := require.New(t)
	s := newTestStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	// Test to create cluster view.
	expectClusterView := ClusterView{
		ClusterID:  defaultClusterID,
		Version:    defaultVersion,
		State:      ClusterStateEmpty,
		ShardNodes: nil,
		CreatedAt:  uint64(time.Now().UnixMilli()),
	}

	req := CreateClusterViewRequest{
		ClusterView: expectClusterView,
	}
	err := s.CreateClusterView(ctx, req)
	re.NoError(err)

	// Test to get cluster view.
	ret, err := s.GetClusterView(ctx, GetClusterViewRequest{
		ClusterID: defaultClusterID,
	})
	re.NoError(err)
	re.Equal(expectClusterView.ClusterID, ret.ClusterView.ClusterID)
	re.Equal(expectClusterView.Version, ret.ClusterView.Version)
	re.Equal(expectClusterView.CreatedAt, ret.ClusterView.CreatedAt)

	// Test to put cluster view.
	expectClusterView.Version = uint64(1)
	putReq := UpdateClusterViewRequest{
		ClusterID:     defaultClusterID,
		ClusterView:   expectClusterView,
		LatestVersion: 0,
	}
	err = s.UpdateClusterView(ctx, putReq)
	re.NoError(err)

	ret, err = s.GetClusterView(ctx, GetClusterViewRequest{
		ClusterID: defaultClusterID,
	})
	re.NoError(err)
	re.Equal(expectClusterView.ClusterID, ret.ClusterView.ClusterID)
	re.Equal(expectClusterView.Version, ret.ClusterView.Version)
	re.Equal(expectClusterView.CreatedAt, ret.ClusterView.CreatedAt)
}

func TestStorage_CreateAndListScheme(t *testing.T) {
	re := require.New(t)
	s := newTestStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	// Test to create expectSchemas.
	expectSchemas := make([]Schema, 0, defaultCount)
	for i := 0; i < defaultCount; i++ {
		schema := Schema{
			ID:        SchemaID(i),
			ClusterID: defaultClusterID,
			Name:      fmt.Sprintf(nameFormat, i),
			CreatedAt: uint64(time.Now().UnixMilli()),
		}
		req := CreateSchemaRequest{
			ClusterID: defaultClusterID,
			Schema:    schema,
		}
		err := s.CreateSchema(ctx, req)
		re.NoError(err)
		expectSchemas = append(expectSchemas, schema)
	}

	// Test to list expectSchemas.
	ret, err := s.ListSchemas(ctx, ListSchemasRequest{
		ClusterID: defaultClusterID,
	})
	re.NoError(err)
	for i := 0; i < defaultCount; i++ {
		re.Equal(expectSchemas[i].ID, ret.Schemas[i].ID)
		re.Equal(expectSchemas[i].ClusterID, ret.Schemas[i].ClusterID)
		re.Equal(expectSchemas[i].Name, ret.Schemas[i].Name)
		re.Equal(expectSchemas[i].CreatedAt, ret.Schemas[i].CreatedAt)
	}
}

func TestStorage_CreateAndGetAndListTable(t *testing.T) {
	re := require.New(t)
	s := newTestStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout*100)
	defer cancel()

	// Test to create tables.
	expectTables := make([]Table, 0, defaultCount)
	for i := 0; i < defaultCount; i++ {
		table := Table{
			ID:       TableID(i),
			Name:     fmt.Sprintf(nameFormat, i),
			SchemaID: defaultSchemaID,
		}
		req := CreateTableRequest{
			ClusterID: defaultClusterID,
			SchemaID:  defaultSchemaID,
			Table:     table,
		}
		err := s.CreateTable(ctx, req)
		re.NoError(err)
		expectTables = append(expectTables, table)
	}

	// Test to get table.
	tableResult, err := s.GetTable(ctx, GetTableRequest{
		ClusterID: defaultClusterID,
		SchemaID:  defaultSchemaID,
		TableName: name0,
	})
	re.NoError(err)
	re.True(tableResult.Exists)
	re.Equal(expectTables[0].ID, tableResult.Table.ID)
	re.Equal(expectTables[0].Name, tableResult.Table.Name)
	re.Equal(expectTables[0].SchemaID, tableResult.Table.SchemaID)
	re.Equal(expectTables[0].CreatedAt, tableResult.Table.CreatedAt)

	// Test to list tables.
	tablesResult, err := s.ListTables(ctx, ListTableRequest{
		ClusterID: defaultClusterID,
		SchemaID:  defaultSchemaID,
	})
	re.NoError(err)

	for i := 0; i < defaultCount; i++ {
		re.True(tableResult.Exists)
		re.Equal(expectTables[i].ID, tablesResult.Tables[i].ID)
		re.Equal(expectTables[i].Name, tablesResult.Tables[i].Name)
		re.Equal(expectTables[i].SchemaID, tablesResult.Tables[i].SchemaID)
		re.Equal(expectTables[i].CreatedAt, tablesResult.Tables[i].CreatedAt)
	}

	// Test to delete table.
	err = s.DeleteTable(ctx, DeleteTableRequest{
		ClusterID: defaultClusterID,
		SchemaID:  defaultSchemaID,
		TableName: name0,
	})
	re.NoError(err)

	tableResult, err = s.GetTable(ctx, GetTableRequest{
		ClusterID: defaultClusterID,
		SchemaID:  defaultSchemaID,
		TableName: name0,
	})
	re.NoError(err)
	re.Empty(tableResult.Table)
	re.True(!tableResult.Exists)
}

func TestStorage_CreateAndListShardView(t *testing.T) {
	re := require.New(t)
	s := newTestStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	// Test to create shard topologies.
	expectShardViews := make([]ShardView, 0, defaultCount)
	var shardIDs []ShardID
	for i := 0; i < defaultCount; i++ {
		shardView := ShardView{
			ShardID:   ShardID(i),
			Version:   defaultVersion,
			TableIDs:  nil,
			CreatedAt: uint64(time.Now().UnixMilli()),
		}
		expectShardViews = append(expectShardViews, shardView)
		shardIDs = append(shardIDs, ShardID(i))
	}
	err := s.CreateShardViews(ctx, CreateShardViewsRequest{
		ClusterID:  defaultClusterID,
		ShardViews: expectShardViews,
	})
	re.NoError(err)

	// Test to list shard topologies.
	ret, err := s.ListShardViews(ctx, ListShardViewsRequest{
		ClusterID: defaultClusterID,
		ShardIDs:  shardIDs,
	})
	re.NoError(err)
	for i := 0; i < defaultCount; i++ {
		re.Equal(expectShardViews[i].ShardID, ret.ShardViews[i].ShardID)
		re.Equal(expectShardViews[i].Version, ret.ShardViews[i].Version)
		re.Equal(expectShardViews[i].CreatedAt, ret.ShardViews[i].CreatedAt)
	}

	newVersion := uint64(1)
	// Test to put shard topologies.
	for i := 0; i < defaultCount; i++ {
		expectShardViews[i].Version = newVersion
		err = s.UpdateShardView(ctx, UpdateShardViewRequest{
			ClusterID:     defaultClusterID,
			ShardView:     expectShardViews[i],
			LatestVersion: defaultVersion,
		})
		re.NoError(err)
	}

	ret, err = s.ListShardViews(ctx, ListShardViewsRequest{
		ClusterID: defaultClusterID,
		ShardIDs:  shardIDs,
	})
	re.NoError(err)
	for i := 0; i < defaultCount; i++ {
		re.Equal(expectShardViews[i].ShardID, ret.ShardViews[i].ShardID)
		re.Equal(expectShardViews[i].Version, ret.ShardViews[i].Version)
		re.Equal(expectShardViews[i].CreatedAt, ret.ShardViews[i].CreatedAt)
	}
}

func TestStorage_CreateOrUpdateNode(t *testing.T) {
	re := require.New(t)
	s := newTestStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	// Test to create nodes.
	expectNodes := make([]Node, 0, defaultCount)
	for i := 0; i < defaultCount; i++ {
		node := Node{
			Name:          fmt.Sprintf(nameFormat, i),
			NodeStats:     NodeStats{},
			LastTouchTime: uint64(time.Now().UnixMilli()),
		}
		err := s.CreateOrUpdateNode(ctx, CreateOrUpdateNodeRequest{
			ClusterID: defaultClusterID,
			Node:      node,
		})
		re.NoError(err)
		expectNodes = append(expectNodes, node)
	}

	// Test to list nodes.
	ret, err := s.ListNodes(ctx, ListNodesRequest{
		ClusterID: defaultClusterID,
	})
	re.NoError(err)

	re.Equal(len(ret.Nodes), defaultCount)
	for i := 0; i < defaultCount; i++ {
		re.Equal(ret.Nodes[i].Name, expectNodes[i].Name)
		re.Equal(ret.Nodes[i].LastTouchTime, expectNodes[i].LastTouchTime)
	}
}

func newTestStorage(t *testing.T) Storage {
	cfg := etcdutil.NewTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)

	<-etcd.Server.ReadyNotify()

	endpoint := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint},
	})
	assert.NoError(t, err)

	ops := Options{MaxScanLimit: 100, MinScanLimit: 10}

	return newEtcdStorage(client, defaultRootPath, ops)
}
