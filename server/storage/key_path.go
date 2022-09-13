// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"fmt"
	"path"
)

const (
	version       = "v1"
	cluster       = "cluster"
	schema        = "schema"
	table         = "table"
	shard         = "shard"
	node          = "node"
	topology      = "topo"
	latestVersion = "latest_version"
	info          = "info"
)

// makeSchemaKey returns the key path to the schema meta info.
func makeSchemaKey(rootPath string, clusterID uint32, schemaID uint32) string {
	// Example:
	//	v1/cluster/1/schema/info/1 -> pb.Schema
	//	v1/cluster/1/schema/info/2 -> pb.Schema
	//	v1/cluster/1/schema/info/3 -> pb.Schema
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), schema, info, fmtID(uint64(schemaID)))
}

// makeClusterKey returns the cluster meta info key path.
func makeClusterKey(rootPath string, clusterID uint32) string {
	// Example:
	//	v1/cluster/info/1 -> pb.Cluster
	//	v1/cluster/info/2 -> pb.Cluster
	//	v1/cluster/info/3 -> pb.Cluster
	return path.Join(rootPath, version, cluster, info, fmtID(uint64(clusterID)))
}

// makeTableKey returns the table meta info key path.
func makeTableKey(rootPath string, clusterID uint32, schemaID uint32, tableID uint64) string {
	// Example:
	//	v1/cluster/1/schema/1/table/1 -> pb.Table
	//	v1/cluster/1/schema/1/table/2 -> pb.Table
	//	v1/cluster/1/schema/1/table/3 -> pb.Table
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), schema, fmtID(uint64(schemaID)), table, fmtID(tableID))
}

// makeShardTopologyKey returns the shard meta info key path.
func makeShardTopologyKey(rootPath string, clusterID uint32, shardID uint32, latestVersion string) string {
	// Example:
	//	v1/cluster/1/shard/1/1 -> pb.Shard
	//	v1/cluster/1/shard/2/1 -> pb.Shard
	//	v1/cluster/1/shard/3/1 -> pb.Shard
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), shard, fmtID(uint64(shardID)), latestVersion)
}

// makeClusterTopologyKey returns the cluster topology meta info key path.
func makeClusterTopologyKey(rootPath string, clusterID uint32, latestVersion string) string {
	// Example:
	//	v1/cluster/1/topo/1 -> pb.ClusterTopology
	//	v1/cluster/1/topo/2 -> pb.ClusterTopology
	//	v1/cluster/1/topo/3 -> pb.ClusterTopology
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), topology, latestVersion)
}

// makeClusterTopologyLatestVersionKey returns the latest version info key path of cluster topology.
func makeClusterTopologyLatestVersionKey(rootPath string, clusterID uint32) string {
	// Example:
	//	v1/cluster/1/topo/latestVersion -> pb.ClusterTopologyLatestVersion
	//	v1/cluster/2/topo/latestVersion -> pb.ClusterTopologyLatestVersion
	//	v1/cluster/3/topo/latestVersion -> pb.ClusterTopologyLatestVersion
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), topology, latestVersion)
}

// makeShardLatestVersionKey returns the latest version info key path of shard.
func makeShardLatestVersionKey(rootPath string, clusterID uint32, shardID uint32) string {
	// Example:
	//	v1/cluster/1/shard/1/latestVersion -> pb.ShardLatestVersion
	//	v1/cluster/1/shard/2/latestVersion -> pb.ShardLatestVersion
	//	v1/cluster/1/shard/3/latestVersion -> pb.ShardLatestVersion
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), shard, fmtID(uint64(shardID)), latestVersion)
}

// makeNodeKey returns the node meta info key path.
func makeNodeKey(rootPath string, clusterID uint32, nodeName string) string {
	// Example:
	//	v1/cluster/1/node/127.0.0.1:8081 -> pb.Node
	//	v1/cluster/1/node/127.0.0.2:8081 -> pb.Node
	//	v1/cluster/1/node/127.0.0.3:8081 -> pb.Node
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), node, nodeName)
}

// makeNameToIDKey return the table id key path.
func makeNameToIDKey(rootPath string, clusterID uint32, schemaID uint32, tableName string) string {
	// Example:
	//	v1/cluster/1/schema/1/table/tableName -> tableID
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), schema, fmtID(uint64(schemaID)), table, tableName)
}

func fmtID(id uint64) string {
	return fmt.Sprintf("%020d", id)
}
