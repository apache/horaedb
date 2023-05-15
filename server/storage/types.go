// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"time"

	"github.com/CeresDB/ceresdbproto/golang/pkg/clusterpb"
)

type (
	ClusterID    uint32
	SchemaID     uint32
	ShardID      uint32
	TableID      uint64
	ClusterState int
	ShardRole    int
	NodeState    int
	TopologyType string
)

const (
	ClusterStateEmpty ClusterState = iota + 1
	ClusterStateStable
	ClusterStatePrepare

	TopologyTypeStatic  = "static"
	TopologyTypeDynamic = "dynamic"
)

const (
	ShardRoleLeader ShardRole = iota + 1
	ShardRoleFollower
)

const (
	NodeStateOnline NodeState = iota + 1
	NodeStateOffline
)

type ListClustersResult struct {
	Clusters []Cluster
}

type CreateClusterRequest struct {
	Cluster Cluster
}

type UpdateClusterRequest struct {
	Cluster Cluster
}

type CreateClusterViewRequest struct {
	ClusterView ClusterView
}

type GetClusterViewRequest struct {
	ClusterID ClusterID
}

type GetClusterViewResult struct {
	ClusterView ClusterView
}

type UpdateClusterViewRequest struct {
	ClusterID     ClusterID
	ClusterView   ClusterView
	LatestVersion uint64
}

type ListSchemasRequest struct {
	ClusterID ClusterID
}

type ListSchemasResult struct {
	Schemas []Schema
}

type CreateSchemaRequest struct {
	ClusterID ClusterID
	Schema    Schema
}

type CreateTableRequest struct {
	ClusterID ClusterID
	SchemaID  SchemaID
	Table     Table
}

type GetTableRequest struct {
	ClusterID ClusterID
	SchemaID  SchemaID
	TableName string
}

type GetTableResult struct {
	Table  Table
	Exists bool
}

type ListTableRequest struct {
	ClusterID ClusterID
	SchemaID  SchemaID
}

type ListTablesResult struct {
	Tables []Table
}

type DeleteTableRequest struct {
	ClusterID ClusterID
	SchemaID  SchemaID
	TableName string
}

type CreateShardViewsRequest struct {
	ClusterID  ClusterID
	ShardViews []ShardView
}

type ListShardViewsRequest struct {
	ClusterID ClusterID
	ShardIDs  []ShardID
}

type ListShardViewsResult struct {
	ShardViews []ShardView
}

type UpdateShardViewRequest struct {
	ClusterID     ClusterID
	ShardView     ShardView
	LatestVersion uint64
}

type ListNodesRequest struct {
	ClusterID ClusterID
}

type ListNodesResult struct {
	Nodes []Node
}

type CreateOrUpdateNodeRequest struct {
	ClusterID ClusterID
	Node      Node
}

type Cluster struct {
	ID           ClusterID
	Name         string
	MinNodeCount uint32
	// Deprecated: ReplicationFactor is deprecated after CeresMeta v1.2.0
	ReplicationFactor uint32
	ShardTotal        uint32
	EnableSchedule    bool
	TopologyType      TopologyType
	CreatedAt         uint64
	ModifiedAt        uint64
}

type ShardNode struct {
	ID        ShardID
	ShardRole ShardRole
	NodeName  string
}

type ClusterView struct {
	ClusterID  ClusterID
	Version    uint64
	State      ClusterState
	ShardNodes []ShardNode
	CreatedAt  uint64
}

func NewClusterView(clusterID ClusterID, version uint64, state ClusterState, shardNodes []ShardNode) ClusterView {
	return ClusterView{
		ClusterID:  clusterID,
		Version:    version,
		State:      state,
		ShardNodes: shardNodes,
		CreatedAt:  uint64(time.Now().UnixMilli()),
	}
}

type Schema struct {
	ID        SchemaID
	ClusterID ClusterID
	Name      string
	CreatedAt uint64
}

type PartitionInfo struct {
	Info *clusterpb.PartitionInfo
}

type Table struct {
	ID            TableID
	Name          string
	SchemaID      SchemaID
	CreatedAt     uint64
	PartitionInfo PartitionInfo
}

func (t Table) IsPartitioned() bool {
	return t.PartitionInfo.Info != nil
}

type ShardView struct {
	ShardID   ShardID
	Version   uint64
	TableIDs  []TableID
	CreatedAt uint64
}

func NewShardView(shardID ShardID, version uint64, tableIDs []TableID) ShardView {
	return ShardView{
		ShardID:   shardID,
		Version:   version,
		TableIDs:  tableIDs,
		CreatedAt: uint64(time.Now().UnixMilli()),
	}
}

type NodeStats struct {
	Lease       uint32
	Zone        string
	NodeVersion string
}

type Node struct {
	Name          string
	NodeStats     NodeStats
	LastTouchTime uint64
	State         NodeState
}

func ConvertShardRolePB(role clusterpb.ShardRole) ShardRole {
	switch role {
	case clusterpb.ShardRole_LEADER:
		return ShardRoleLeader
	case clusterpb.ShardRole_FOLLOWER:
		return ShardRoleFollower
	}
	return ShardRoleFollower
}

func convertNodeToPB(node Node) clusterpb.Node {
	nodeStats := convertNodeStatsToPB(node.NodeStats)
	return clusterpb.Node{
		Name:          node.Name,
		Stats:         &nodeStats,
		LastTouchTime: node.LastTouchTime,
		State:         convertNodeStateToPB(node.State),
	}
}

func convertClusterPB(cluster *clusterpb.Cluster) Cluster {
	return Cluster{
		ID:             ClusterID(cluster.Id),
		Name:           cluster.Name,
		MinNodeCount:   cluster.MinNodeCount,
		ShardTotal:     cluster.ShardTotal,
		EnableSchedule: cluster.EnableSchedule,
		TopologyType:   convertTopologyTypePB(cluster.TopologyType),
		CreatedAt:      cluster.CreatedAt,
		ModifiedAt:     cluster.ModifiedAt,
	}
}

func convertClusterToPB(cluster Cluster) clusterpb.Cluster {
	return clusterpb.Cluster{
		Id:             uint32(cluster.ID),
		Name:           cluster.Name,
		MinNodeCount:   cluster.MinNodeCount,
		ShardTotal:     cluster.ShardTotal,
		EnableSchedule: cluster.EnableSchedule,
		TopologyType:   convertTopologyTypeToPB(cluster.TopologyType),
		CreatedAt:      cluster.CreatedAt,
		ModifiedAt:     cluster.ModifiedAt,
	}
}

func convertTopologyTypeToPB(topologyType TopologyType) clusterpb.Cluster_TopologyType {
	switch topologyType {
	case TopologyTypeStatic:
		return clusterpb.Cluster_STATIC
	case TopologyTypeDynamic:
		return clusterpb.Cluster_DYNAMIC
	}
	return clusterpb.Cluster_STATIC
}

func convertTopologyTypePB(topologyType clusterpb.Cluster_TopologyType) TopologyType {
	switch topologyType {
	case clusterpb.Cluster_STATIC:
		return TopologyTypeStatic
	case clusterpb.Cluster_DYNAMIC:
		return TopologyTypeDynamic
	}
	return TopologyTypeStatic
}

func convertClusterStateToPB(state ClusterState) clusterpb.ClusterView_ClusterState {
	switch state {
	case ClusterStateEmpty:
		return clusterpb.ClusterView_EMPTY
	case ClusterStateStable:
		return clusterpb.ClusterView_STABLE
	case ClusterStatePrepare:
		return clusterpb.ClusterView_PREPARE_REBALANCE
	}
	return clusterpb.ClusterView_EMPTY
}

func convertClusterStatePB(state clusterpb.ClusterView_ClusterState) ClusterState {
	switch state {
	case clusterpb.ClusterView_EMPTY:
		return ClusterStateEmpty
	case clusterpb.ClusterView_STABLE:
		return ClusterStateStable
	case clusterpb.ClusterView_PREPARE_REBALANCE:
		return ClusterStatePrepare
	}
	return ClusterStateEmpty
}

func ConvertShardRoleToPB(role ShardRole) clusterpb.ShardRole {
	switch role {
	case ShardRoleLeader:
		return clusterpb.ShardRole_LEADER
	case ShardRoleFollower:
		return clusterpb.ShardRole_FOLLOWER
	}
	return clusterpb.ShardRole_FOLLOWER
}

func convertShardNodeToPB(shardNode ShardNode) clusterpb.ShardNode {
	return clusterpb.ShardNode{
		Id:        uint32(shardNode.ID),
		ShardRole: ConvertShardRoleToPB(shardNode.ShardRole),
		Node:      shardNode.NodeName,
	}
}

func convertShardNodePB(shardNode *clusterpb.ShardNode) ShardNode {
	return ShardNode{
		ID:        ShardID(shardNode.Id),
		ShardRole: ConvertShardRolePB(shardNode.ShardRole),
		NodeName:  shardNode.Node,
	}
}

func convertClusterViewToPB(view ClusterView) clusterpb.ClusterView {
	shardViews := make([]*clusterpb.ShardNode, 0, len(view.ShardNodes))
	for _, shardNode := range view.ShardNodes {
		shardNodePB := convertShardNodeToPB(shardNode)
		shardViews = append(shardViews, &shardNodePB)
	}

	return clusterpb.ClusterView{
		ClusterId:  uint32(view.ClusterID),
		Version:    view.Version,
		State:      convertClusterStateToPB(view.State),
		ShardNodes: shardViews,
		Cause:      "",
		CreatedAt:  view.CreatedAt,
	}
}

func convertClusterViewPB(view *clusterpb.ClusterView) ClusterView {
	shardNodes := make([]ShardNode, 0, len(view.ShardNodes))
	for _, shardNodePB := range view.ShardNodes {
		shardNode := convertShardNodePB(shardNodePB)
		shardNodes = append(shardNodes, shardNode)
	}

	return ClusterView{
		ClusterID:  ClusterID(view.ClusterId),
		Version:    view.Version,
		State:      convertClusterStatePB(view.State),
		ShardNodes: shardNodes,
		CreatedAt:  view.CreatedAt,
	}
}

func convertSchemaToPB(schema Schema) clusterpb.Schema {
	return clusterpb.Schema{
		Id:        uint32(schema.ID),
		ClusterId: uint32(schema.ClusterID),
		Name:      schema.Name,
		CreatedAt: schema.CreatedAt,
	}
}

func convertSchemaPB(schema *clusterpb.Schema) Schema {
	return Schema{
		ID:        SchemaID(schema.Id),
		ClusterID: ClusterID(schema.ClusterId),
		Name:      schema.Name,
		CreatedAt: schema.CreatedAt,
	}
}

func convertTableToPB(table Table) clusterpb.Table {
	return clusterpb.Table{
		Id:            uint64(table.ID),
		Name:          table.Name,
		SchemaId:      uint32(table.SchemaID),
		Desc:          "",
		CreatedAt:     table.CreatedAt,
		PartitionInfo: table.PartitionInfo.Info,
	}
}

func convertTablePB(table *clusterpb.Table) Table {
	return Table{
		ID:        TableID(table.Id),
		Name:      table.Name,
		SchemaID:  SchemaID(table.SchemaId),
		CreatedAt: table.CreatedAt,
		PartitionInfo: PartitionInfo{
			Info: table.PartitionInfo,
		},
	}
}

func convertShardViewToPB(view ShardView) clusterpb.ShardView {
	tableIDs := make([]uint64, 0, len(view.TableIDs))
	for _, id := range view.TableIDs {
		tableIDs = append(tableIDs, uint64(id))
	}

	return clusterpb.ShardView{
		ShardId:   uint32(view.ShardID),
		TableIds:  tableIDs,
		Version:   view.Version,
		CreatedAt: view.CreatedAt,
	}
}

func convertShardViewPB(shardTopology *clusterpb.ShardView) ShardView {
	tableIDs := make([]TableID, 0, len(shardTopology.TableIds))
	for _, id := range shardTopology.TableIds {
		tableIDs = append(tableIDs, TableID(id))
	}

	return ShardView{
		ShardID:   ShardID(shardTopology.ShardId),
		Version:   shardTopology.Version,
		TableIDs:  tableIDs,
		CreatedAt: shardTopology.CreatedAt,
	}
}

func convertNodeStatsToPB(stats NodeStats) clusterpb.NodeStats {
	return clusterpb.NodeStats{
		Lease:       stats.Lease,
		Zone:        stats.Zone,
		NodeVersion: stats.NodeVersion,
	}
}

func convertNodeStatsPB(stats *clusterpb.NodeStats) NodeStats {
	return NodeStats{
		Lease:       stats.Lease,
		Zone:        stats.Zone,
		NodeVersion: stats.NodeVersion,
	}
}

func convertNodeStateToPB(state NodeState) clusterpb.NodeState {
	switch state {
	case NodeStateOnline:
		return clusterpb.NodeState_ONLINE
	case NodeStateOffline:
		return clusterpb.NodeState_OFFLINE
	}
	return clusterpb.NodeState_OFFLINE
}

func convertNodeStatePB(state clusterpb.NodeState) NodeState {
	switch state {
	case clusterpb.NodeState_ONLINE:
		return NodeStateOnline
	case clusterpb.NodeState_OFFLINE:
		return NodeStateOffline
	}
	return NodeStateOffline
}

func convertNodePB(node *clusterpb.Node) Node {
	nodeStats := convertNodeStatsPB(node.Stats)
	return Node{
		Name:          node.Name,
		NodeStats:     nodeStats,
		LastTouchTime: node.LastTouchTime,
		State:         convertNodeStatePB(node.State),
	}
}
