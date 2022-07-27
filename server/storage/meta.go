// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
)

// MetaStorage defines the storage operations on the ceresdb cluster meta info.
type MetaStorage interface {
	ListClusters(ctx context.Context) ([]*clusterpb.Cluster, error)
	CreateCluster(ctx context.Context, cluster *clusterpb.Cluster) (*clusterpb.Cluster, error)
	GetCluster(ctx context.Context, clusterID uint32) (*clusterpb.Cluster, error)
	PutCluster(ctx context.Context, clusterID uint32, meta *clusterpb.Cluster) error

	CreateClusterTopology(ctx context.Context, clusterTopology *clusterpb.ClusterTopology) error
	GetClusterTopology(ctx context.Context, clusterID uint32) (*clusterpb.ClusterTopology, error)
	PutClusterTopology(ctx context.Context, clusterID uint32, latestVersion uint32, clusterMetaData *clusterpb.ClusterTopology) error

	ListSchemas(ctx context.Context, clusterID uint32) ([]*clusterpb.Schema, error)
	CreateSchema(ctx context.Context, clusterID uint32, schema *clusterpb.Schema) error
	PutSchemas(ctx context.Context, clusterID uint32, schemas []*clusterpb.Schema) error

	CreateTable(ctx context.Context, clusterID uint32, schemaID uint32, table *clusterpb.Table) error
	GetTable(ctx context.Context, clusterID uint32, schemaID uint32, tableName string) (*clusterpb.Table, bool, error)
	ListTables(ctx context.Context, clusterID uint32, schemaID uint32) ([]*clusterpb.Table, error)
	PutTables(ctx context.Context, clusterID uint32, schemaID uint32, tables []*clusterpb.Table) error
	DeleteTables(ctx context.Context, clusterID uint32, schemaID uint32, tableIDs []uint64) error

	ListShardTopologies(ctx context.Context, clusterID uint32, shardID []uint32) ([]*clusterpb.ShardTopology, error)
	PutShardTopologies(ctx context.Context, clusterID uint32, shardIDs []uint32, latestVersion uint32, topologies []*clusterpb.ShardTopology) error

	ListNodes(ctx context.Context, clusterID uint32) ([]*clusterpb.Node, error)
	PutNodes(ctx context.Context, clusterID uint32, node []*clusterpb.Node) error
	CreateOrUpdateNode(ctx context.Context, clusterID uint32, node *clusterpb.Node) (*clusterpb.Node, error)
}
