// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/assert"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/hash"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
)

type NodePicker interface {
	PickNode(ctx context.Context, shardIDs []storage.ShardID, shardTotalNum uint32, registerNodes []metadata.RegisteredNode) (map[storage.ShardID]metadata.RegisteredNode, error)
}

type ConsistentUniformHashNodePicker struct {
	logger *zap.Logger
}

func NewConsistentUniformHashNodePicker(logger *zap.Logger) NodePicker {
	return &ConsistentUniformHashNodePicker{logger: logger}
}

type nodeMember string

var _ hash.Member = nodeMember("")

func (m nodeMember) String() string {
	return string(m)
}

const uniformHashReplicationFactor int = 127

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return murmur3.Sum64(data)
}

// filterExpiredNodes will retain the alive nodes only.
func filterExpiredNodes(nodes []metadata.RegisteredNode) map[string]metadata.RegisteredNode {
	now := time.Now()

	aliveNodes := make(map[string]metadata.RegisteredNode, len(nodes))
	for _, node := range nodes {
		if !node.IsExpired(now) {
			aliveNodes[node.Node.Name] = node
		}
	}

	return aliveNodes
}

func (p *ConsistentUniformHashNodePicker) PickNode(_ context.Context, shardIDs []storage.ShardID, shardTotalNum uint32, registerNodes []metadata.RegisteredNode) (map[storage.ShardID]metadata.RegisteredNode, error) {
	aliveNodes := filterExpiredNodes(registerNodes)
	if len(aliveNodes) == 0 {
		return nil, ErrPickNode.WithCausef("no alive node in cluster")
	}

	mems := make([]hash.Member, 0, len(aliveNodes))
	for _, node := range registerNodes {
		if _, alive := aliveNodes[node.Node.Name]; alive {
			mems = append(mems, nodeMember(node.Node.Name))
		}
	}

	conf := hash.Config{
		ReplicationFactor: uniformHashReplicationFactor,
		Hasher:            hasher{},
	}
	h, err := hash.BuildConsistentUniformHash(int(shardTotalNum), mems, conf)
	if err != nil {
		return nil, ErrPickNode.WithCause(err)
	}

	shardNodes := make(map[storage.ShardID]metadata.RegisteredNode, len(registerNodes))
	for _, shardID := range shardIDs {
		assert.Assert(shardID < storage.ShardID(shardTotalNum))
		partID := int(shardID)
		nodeName := h.GetPartitionOwner(partID).String()
		node, ok := aliveNodes[nodeName]
		assert.Assertf(ok, "node:%s must be in the aliveNodes:%v", nodeName, aliveNodes)
		shardNodes[storage.ShardID(partID)] = node

		p.logger.Debug("shard is allocated to the node", zap.Uint32("shardID", uint32(shardID)), zap.String("node", nodeName))
	}

	return shardNodes, nil
}
