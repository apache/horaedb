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
	tableNameToID = "table_name_to_id"
	node          = "node"
	clusterView   = "cluster_view"
	shardView     = "shard_view"
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

// makeClusterViewLatestVersionKey returns the latest version info key path of cluster clusterView.
func makeClusterViewLatestVersionKey(rootPath string, clusterID uint32) string {
	// Example:
	//	v1/cluster/1/clusterView/latest_version -> pb.ClusterTopologyLatestVersion
	//	v1/cluster/2/clusterView/latest_version -> pb.ClusterTopologyLatestVersion
	//	v1/cluster/3/clusterView/latest_version -> pb.ClusterTopologyLatestVersion
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), clusterView, latestVersion)
}

// makeClusterViewKey returns the cluster view meta info key path.
func makeClusterViewKey(rootPath string, clusterID uint32, latestVersion string) string {
	// Example:
	//	v1/cluster/1/clusterView/1 -> pb.ClusterTopology
	//	v1/cluster/1/clusterView/2 -> pb.ClusterTopology
	//	v1/cluster/1/clusterView/3 -> pb.ClusterTopology
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), clusterView, latestVersion)
}

// makeShardViewLatestVersionKey returns the latest version info key path of shard.
func makeShardViewLatestVersionKey(rootPath string, clusterID uint32, shardID uint32) string {
	// Example:
	//	v1/cluster/1/shard_view/1/latest_version -> pb.ShardLatestVersion
	//	v1/cluster/1/shard_view/2/latest_version -> pb.ShardLatestVersion
	//	v1/cluster/1/shard_view/3/latest_version -> pb.ShardLatestVersion
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), shardView, fmtID(uint64(shardID)), latestVersion)
}

// makeShardViewKey returns the shard meta info key path.
func makeShardViewKey(rootPath string, clusterID uint32, shardID uint32, latestVersion string) string {
	// Example:
	//	v1/cluster/1/shard_view/1/1 -> pb.ShardTopology
	//	v1/cluster/1/shard_view/2/1 -> pb.ShardTopology
	//	v1/cluster/1/shard_view/3/1 -> pb.ShardTopology
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), shardView, fmtID(uint64(shardID)), latestVersion)
}

// makeNodeKey returns the node meta info key path.
func makeNodeKey(rootPath string, clusterID uint32, nodeName string) string {
	// Example:
	//	v1/cluster/1/node/127.0.0.1:8081 -> pb.NodeName
	//	v1/cluster/1/node/127.0.0.2:8081 -> pb.NodeName
	//	v1/cluster/1/node/127.0.0.3:8081 -> pb.NodeName
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), node, nodeName)
}

// makeTableKey returns the table meta info key path.
func makeTableKey(rootPath string, clusterID uint32, schemaID uint32, tableID uint64) string {
	// Example:
	//	v1/cluster/1/schema/1/table/1 -> pb.Table
	//	v1/cluster/1/schema/1/table/2 -> pb.Table
	//	v1/cluster/1/schema/1/table/3 -> pb.Table
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), schema, fmtID(uint64(schemaID)), table, fmtID(tableID))
}

// makeNameToIDKey return the table id key path.
func makeNameToIDKey(rootPath string, clusterID uint32, schemaID uint32, tableName string) string {
	// Example:
	//	v1/cluster/1/schema/1/table_name_to_id/table1 -> 1
	//	v1/cluster/1/schema/1/table_name_to_id/table2 -> 2
	return path.Join(rootPath, version, cluster, fmtID(uint64(clusterID)), schema, fmtID(uint64(schemaID)), tableNameToID, tableName)
}

func fmtID(id uint64) string {
	return fmt.Sprintf("%020d", id)
}
