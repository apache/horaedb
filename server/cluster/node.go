// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import "github.com/CeresDB/ceresdbproto/pkg/clusterpb"

type Node struct {
	meta     *clusterpb.Node
	shardIDs []uint32
}

func (n Node) GetShardIDs() []uint32 {
	return n.shardIDs
}

func (n Node) GetMeta() *clusterpb.Node {
	return n.meta
}
