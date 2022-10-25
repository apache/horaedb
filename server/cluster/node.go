// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import "github.com/CeresDB/ceresdbproto/pkg/clusterpb"

type RegisteredNode struct {
	meta       *clusterpb.Node
	shardInfos []*ShardInfo
}

func NewRegisteredNode(meta *clusterpb.Node, shardInfos []*ShardInfo) *RegisteredNode {
	return &RegisteredNode{
		meta,
		shardInfos,
	}
}

func (n *RegisteredNode) GetShardInfos() []*ShardInfo {
	return n.shardInfos
}

func (n *RegisteredNode) GetMeta() *clusterpb.Node {
	return n.meta
}

func (n *RegisteredNode) IsOnline() bool {
	return n.meta.State == clusterpb.NodeState_ONLINE
}
