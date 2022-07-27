// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
)

// MetaStorage defines the storage operations on the ceresdb cluster meta info.
type MetaStorage interface {
	GetCluster(ctx context.Context, clusterID uint32) (*metapb.Cluster, error)
	PutCluster(ctx context.Context, clusterID uint32, meta *metapb.Cluster) error

	GetClusterTopology(ctx context.Context, clusterID uint32) (*metapb.ClusterTopology, error)
	PutClusterTopology(ctx context.Context, clusterID uint32, latestVersion uint32, clusterMetaData *metapb.ClusterTopology) error

	ListSchemas(ctx context.Context, clusterID uint32) ([]*metapb.Schema, error)
	PutSchemas(ctx context.Context, clusterID uint32, schemas []*metapb.Schema) error

	ListTables(ctx context.Context, clusterID uint32, schemaID uint32, tableIDs []uint64) ([]*metapb.Table, error)
	PutTables(ctx context.Context, clusterID uint32, schemaID uint32, tables []*metapb.Table) error
	DeleteTables(ctx context.Context, clusterID uint32, schemaID uint32, tableIDs []uint64) error

	ListShardTopologies(ctx context.Context, clusterID uint32, shardIDs []uint32) ([]*metapb.ShardTopology, error)
	PutShardTopologies(ctx context.Context, clusterID uint32, shardIDs []uint32, latestVersion uint32, topologies []*metapb.ShardTopology) error

	ListNodes(ctx context.Context, clusterID uint32) ([]*metapb.Node, error)
	PutNodes(ctx context.Context, clusterID uint32, nodes []*metapb.Node) error
}
