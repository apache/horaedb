// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/hash"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
)

type NodePicker interface {
	PickNode(ctx context.Context, shardIDs []storage.ShardID, shardTotalNum uint32, registerNodes []metadata.RegisteredNode) (map[storage.ShardID]metadata.RegisteredNode, error)
}
type UniformityConsistentHashNodePicker struct {
	logger *zap.Logger
}

func NewUniformityConsistentHashNodePicker(logger *zap.Logger) NodePicker {
	return &UniformityConsistentHashNodePicker{logger: logger}
}

type nodeMember string

var _ hash.Member = nodeMember("")

func (m nodeMember) String() string {
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return murmur3.Sum64(data)
}

func (p *UniformityConsistentHashNodePicker) PickNode(_ context.Context, shardIDs []storage.ShardID, shardTotalNum uint32, registerNodes []metadata.RegisteredNode) (map[storage.ShardID]metadata.RegisteredNode, error) {
	now := time.Now()

	result := make(map[storage.ShardID]metadata.RegisteredNode, len(registerNodes))
	aliveNodes := make([]metadata.RegisteredNode, 0, len(registerNodes))
	for _, registerNode := range registerNodes {
		if !registerNode.IsExpired(now) {
			aliveNodes = append(aliveNodes, registerNode)
		}
	}
	if len(aliveNodes) == 0 {
		return map[storage.ShardID]metadata.RegisteredNode{}, errors.WithMessage(ErrPickNode, "no alive node in cluster")
	}

	mems := make([]hash.Member, 0, len(aliveNodes))
	for _, node := range aliveNodes {
		mems = append(mems, nodeMember(node.Node.Name))
	}

	consistentConfig := hash.Config{
		PartitionCount:    int(shardTotalNum),
		ReplicationFactor: int(shardTotalNum),
		Load:              1,
		Hasher:            hasher{},
	}
	c := hash.NewUniformityHash(mems, consistentConfig)
	for partID := 0; partID < consistentConfig.PartitionCount; partID++ {
		mem := c.GetPartitionOwner(partID)
		nodeName := mem.String()

		if contains(storage.ShardID(partID), shardIDs) {
			p.logger.Debug("shard is founded in members", zap.Uint32("shardID", uint32(partID)), zap.String("memberName", mem.String()))
			for _, node := range aliveNodes {
				if node.Node.Name == nodeName {
					result[storage.ShardID(partID)] = node
				}
			}
		}
	}

	if len(result) != len(shardIDs) {
		return map[storage.ShardID]metadata.RegisteredNode{}, errors.WithMessagef(ErrPickNode, "length of the result is inconsistent with the length of the shard, shardID:%d, shardTotalNum:%d, aliveNodeNum:%d", shardIDs, shardTotalNum, len(aliveNodes))
	}

	return result, nil
}

func contains(target storage.ShardID, shardIDs []storage.ShardID) bool {
	for i := 0; i < len(shardIDs); i++ {
		if shardIDs[i] == target {
			return true
		}
	}
	return false
}
