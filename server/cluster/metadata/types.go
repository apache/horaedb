// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package metadata

import (
	"time"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

const (
	expiredThreshold                     = time.Second * 10
	MinShardID                           = 0
	HeartbeatKeepAliveIntervalSec uint64 = 15
)

type Snapshot struct {
	Topology        Topology
	RegisteredNodes []RegisteredNode
}

type TableInfo struct {
	ID            storage.TableID
	Name          string
	SchemaID      storage.SchemaID
	SchemaName    string
	PartitionInfo storage.PartitionInfo
}

type ShardTables struct {
	Shard  ShardInfo
	Tables []TableInfo
}

type ShardInfo struct {
	ID   storage.ShardID
	Role storage.ShardRole
	// ShardViewVersion
	Version uint64
}

type ShardNodeWithVersion struct {
	ShardInfo ShardInfo
	ShardNode storage.ShardNode
}

type CreateClusterOpts struct {
	NodeCount                   uint32
	ReplicationFactor           uint32
	ShardTotal                  uint32
	EnableSchedule              bool
	TopologyType                storage.TopologyType
	ProcedureExecutingBatchSize uint32
}

type UpdateClusterOpts struct {
	EnableSchedule              bool
	TopologyType                storage.TopologyType
	ProcedureExecutingBatchSize uint32
}

type CreateTableMetadataRequest struct {
	SchemaName    string
	TableName     string
	PartitionInfo storage.PartitionInfo
}

type CreateTableMetadataResult struct {
	Table storage.Table
}

type CreateTableRequest struct {
	ShardID       storage.ShardID
	SchemaName    string
	TableName     string
	PartitionInfo storage.PartitionInfo
}

type CreateTableResult struct {
	Table              storage.Table
	ShardVersionUpdate ShardVersionUpdate
}

type DropTableResult struct {
	ShardVersionUpdate []ShardVersionUpdate
}

type DropTableMetadataResult struct {
	Table storage.Table
}

type OpenTableRequest struct {
	SchemaName string
	TableName  string
	ShardID    storage.ShardID
	NodeName   string
}

type CloseTableRequest struct {
	SchemaName string
	TableName  string
	ShardID    storage.ShardID
	NodeName   string
}

type MigrateTableRequest struct {
	SchemaName string
	TableNames []string
	OldShardID storage.ShardID
	NewShardID storage.ShardID
}

type ShardVersionUpdate struct {
	ShardID     storage.ShardID
	CurrVersion uint64
	PrevVersion uint64
}

type RouteEntry struct {
	Table      TableInfo
	NodeShards []ShardNodeWithVersion
}

type RouteTablesResult struct {
	ClusterViewVersion uint64
	RouteEntries       map[string]RouteEntry
}

type GetNodeShardsResult struct {
	ClusterTopologyVersion uint64
	NodeShards             []ShardNodeWithVersion
}

type RegisteredNode struct {
	Node       storage.Node
	ShardInfos []ShardInfo
}

func NewRegisteredNode(meta storage.Node, shardInfos []ShardInfo) RegisteredNode {
	return RegisteredNode{
		meta,
		shardInfos,
	}
}

func (n RegisteredNode) IsExpired(now time.Time) bool {
	expiredTime := time.UnixMilli(int64(n.Node.LastTouchTime)).Add(expiredThreshold)

	return now.After(expiredTime)
}

func ConvertShardsInfoToPB(shard ShardInfo) *metaservicepb.ShardInfo {
	return &metaservicepb.ShardInfo{
		Id:      uint32(shard.ID),
		Role:    storage.ConvertShardRoleToPB(shard.Role),
		Version: shard.Version,
	}
}

func ConvertShardsInfoPB(shard *metaservicepb.ShardInfo) ShardInfo {
	return ShardInfo{
		ID:      storage.ShardID(shard.Id),
		Role:    storage.ConvertShardRolePB(shard.Role),
		Version: shard.Version,
	}
}

func ConvertTableInfoToPB(table TableInfo) *metaservicepb.TableInfo {
	return &metaservicepb.TableInfo{
		Id:            uint64(table.ID),
		Name:          table.Name,
		SchemaId:      uint32(table.SchemaID),
		SchemaName:    table.SchemaName,
		PartitionInfo: table.PartitionInfo.Info,
	}
}

func ParseTopologyType(rawString string) (storage.TopologyType, error) {
	switch rawString {
	case storage.TopologyTypeStatic:
		return storage.TopologyTypeStatic, nil
	case storage.TopologyTypeDynamic:
		return storage.TopologyTypeDynamic, nil
	}

	return "", errors.WithMessagef(ErrParseTopologyType, "could not be parsed to topologyType, rawString:%s", rawString)
}
