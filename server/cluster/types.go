// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
)

type TableInfo struct {
	ID         uint64
	Name       string
	SchemaID   uint32
	SchemaName string
}

type ShardTables struct {
	Shard  *ShardInfo
	Tables []*TableInfo
}

type ShardInfo struct {
	ID      uint32
	Role    clusterpb.ShardRole
	Version uint64
}

type ShardsOfNode struct {
	Endpoint string
	ShardIDs []uint32
}

type NodeShard struct {
	Endpoint  string
	ShardInfo *ShardInfo
}

type CreateTableResult struct {
	Table              *Table
	ShardVersionUpdate *ShardVersionUpdate
}

type DropTableResult struct {
	ShardVersionUpdate *ShardVersionUpdate
}

type ShardVersionUpdate struct {
	ShardID     uint32
	CurrVersion uint64
	PrevVersion uint64
}

type RouteEntry struct {
	Table      *TableInfo
	NodeShards []*NodeShard
}

type RouteTablesResult struct {
	Version      uint64
	RouteEntries map[string]*RouteEntry
}

type GetNodeShardsResult struct {
	ClusterTopologyVersion uint64
	NodeShards             []*NodeShard
}

func ConvertShardsInfoToPB(shard *ShardInfo) *metaservicepb.ShardInfo {
	return &metaservicepb.ShardInfo{
		Id:      shard.ID,
		Role:    shard.Role,
		Version: shard.Version,
	}
}

func ConvertTableInfoToPB(table *TableInfo) *metaservicepb.TableInfo {
	return &metaservicepb.TableInfo{
		Id:         table.ID,
		Name:       table.Name,
		SchemaId:   table.SchemaID,
		SchemaName: table.SchemaName,
	}
}

func ConvertNodeToPB(node *RegisteredNode) *clusterpb.Node {
	return &clusterpb.Node{
		Name:                  node.meta.Name,
		NodeStats:             node.meta.NodeStats,
		CreateTime:            node.meta.CreateTime,
		LastTouchTime:         node.meta.LastTouchTime,
		State:                 node.meta.State,
		HeartbeatSamplingInfo: node.meta.HeartbeatSamplingInfo,
	}
}
