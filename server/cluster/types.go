// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"github.com/CeresDB/ceresdbproto/golang/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/storage"
)

const (
	MinShardID = 0
)

type TableInfo struct {
	ID         storage.TableID
	Name       string
	SchemaID   storage.SchemaID
	SchemaName string
}

type ShardTables struct {
	Shard  ShardInfo
	Tables []TableInfo
}

type ShardInfo struct {
	ID      storage.ShardID
	Role    storage.ShardRole
	Version uint64
}

type ShardNodeWithVersion struct {
	ShardInfo ShardInfo
	ShardNode storage.ShardNode
}

type CreateTableResult struct {
	Table              storage.Table
	ShardVersionUpdate ShardVersionUpdate
}

type DropTableResult struct {
	ShardVersionUpdate ShardVersionUpdate
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

func (n *RegisteredNode) IsOnline() bool {
	return n.Node.State == storage.NodeStateOnline
}

func (n RegisteredNode) IsExpired(now uint64, aliveThreshold uint64) bool {
	return now >= aliveThreshold+n.Node.LastTouchTime
}

func ConvertShardsInfoToPB(shard ShardInfo) *metaservicepb.ShardInfo {
	return &metaservicepb.ShardInfo{
		Id:      uint32(shard.ID),
		Role:    clusterpb.ShardRole(shard.Role),
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
		Id:         uint64(table.ID),
		Name:       table.Name,
		SchemaId:   uint32(table.SchemaID),
		SchemaName: table.SchemaName,
	}
}
