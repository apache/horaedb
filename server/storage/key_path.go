// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"fmt"
	"path"
)

const (
	cluster       = "v1/cluster"
	schema        = "schema"
	node          = "node"
	table         = "table"
	shard         = "shard"
	topology      = "topo"
	latestVersion = "latest_version"
)

// makeSchemaKey returns the schema meta info key path.
// example:
// cluster 1: v1/cluster/1/schema/1 -> pb.Schema
//            v1/cluster/1/schema/2 -> pb.Schema
//            v1/cluster/1/schema/3 -> pb.Schema
func makeSchemaKey(clusterID uint32, schemaID uint32) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), schema, fmtID(uint64(schemaID)))
}

// makeNodeKey returns the node meta info key path.
// example:
// cluster 1: v1/cluster/1/node/1 -> pb.Node
//            v1/cluster/1/node/2 -> pb.Node
//            v1/cluster/1/node/3 -> pb.Node
func makeNodeKey(clusterID uint32, nodeID uint32) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), node, fmtID(uint64(nodeID)))
}

// makeClusterKey returns the cluster meta info key path.
// example: v1/cluster/1 -> pb.Cluster
//          v1/cluster/2 -> pb.Cluster
//          v1/cluster/3 -> pb.Cluster
func makeClusterKey(clusterID uint32) string {
	return path.Join(cluster, fmtID(uint64(clusterID)))
}

// makeTableKey returns the table meta info key path.
// example:
// cluster 1: v1/cluster/1/schema/1/table/1 -> pb.Table
//            v1/cluster/1/schema/1/table/2 -> pb.Table
//            v1/cluster/1/schema/1/table/3 -> pb.Table
func makeTableKey(clusterID uint32, schemaID uint32, tableID uint64) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), schema, fmtID(uint64(schemaID)), table, fmtID(tableID))
}

// makeShardKey returns the shard meta info key path.
// example:
// cluster 1: v1/cluster/1/shard/1/1 -> pb.Shard
//            v1/cluster/1/shard/2/1 -> pb.Shard
//            v1/cluster/1/shard/3/1 -> pb.Shard
func makeShardKey(clusterID uint32, shardID uint32, latestVersion string) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), shard, fmtID(uint64(shardID)), latestVersion)
}

// makeClusterTopologyKey returns the cluster topology meta info key path.
// example:
// cluster 1: v1/cluster/1/topo/1 -> pb.ClusterTopology
//            v1/cluster/1/topo/2 -> pb.ClusterTopology
//            v1/cluster/1/topo/3 -> pb.ClusterTopology
func makeClusterTopologyKey(clusterID uint32, latestVersion string) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), topology, latestVersion)
}

// makeClusterTopologyLatestVersionKey returns the latest version info key path of cluster topology.
// example: v1/cluster/1/topo/latestVersion -> pb.ClusterTopologyLatestVersion
//          v1/cluster/2/topo/latestVersion -> pb.ClusterTopologyLatestVersion
//          v1/cluster/3/topo/latestVersion -> pb.ClusterTopologyLatestVersion
func makeClusterTopologyLatestVersionKey(clusterID uint32) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), topology, latestVersion)
}

// makeShardLatestVersionKey returns the latest version info key path of shard.
// example:
// cluster 1: v1/cluster/1/shard/1/latestVersion -> pb.ShardLatestVersion
//            v1/cluster/1/shard/2/latestVersion -> pb.ShardLatestVersion
//            v1/cluster/1/shard/3/latestVersion -> pb.ShardLatestVersion
func makeShardLatestVersionKey(clusterID uint32, shardID uint32) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), shard, fmtID(uint64(shardID)), latestVersion)
}

func fmtID(id uint64) string {
	return fmt.Sprintf("%020d", id)
}
