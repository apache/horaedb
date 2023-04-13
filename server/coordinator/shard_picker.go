// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"crypto/rand"
	"math/big"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

// ShardPicker is used to pick up the shards suitable for scheduling in the cluster.
// If expectShardNum bigger than cluster node number, the result depends on enableDuplicateNode:
// If enableDuplicateNode is false, pick shards will be failed and return error.
// If enableDuplicateNode is true, pick shard will return shards on the same node.
// TODO: Consider refactor this interface, abstracts the parameters of PickShards as PickStrategy.
type ShardPicker interface {
	PickShards(ctx context.Context, snapshot metadata.Snapshot, expectShardNum int, enableDuplicateNode bool) ([]storage.ShardNode, error)
}

// RandomBalancedShardPicker randomly pick up shards that are not on the same node in the current cluster.
type RandomBalancedShardPicker struct{}

func NewRandomBalancedShardPicker() ShardPicker {
	return &RandomBalancedShardPicker{}
}

// PickShards will pick a specified number of shards as expectShardNum.
func (p *RandomBalancedShardPicker) PickShards(_ context.Context, snapshot metadata.Snapshot, expectShardNum int, enableDuplicateNode bool) ([]storage.ShardNode, error) {
	shardNodes := snapshot.Topology.ClusterView.ShardNodes

	nodeShardsMapping := make(map[string][]storage.ShardNode, 0)
	for _, shardNode := range shardNodes {
		_, exists := nodeShardsMapping[shardNode.NodeName]
		if !exists {
			shardNodes := []storage.ShardNode{}
			nodeShardsMapping[shardNode.NodeName] = shardNodes
		}
		nodeShardsMapping[shardNode.NodeName] = append(nodeShardsMapping[shardNode.NodeName], shardNode)
	}

	if !enableDuplicateNode {
		if len(nodeShardsMapping) < expectShardNum {
			return nil, errors.WithMessagef(ErrNodeNumberNotEnough, "number of nodes is:%d, expecet number of shards is:%d", len(nodeShardsMapping), expectShardNum)
		}
	}

	// Try to make shards on different nodes.
	result := make([]storage.ShardNode, 0, expectShardNum)
	totalShardLength := len(shardNodes)
	tempNodeShardMapping := make(map[string][]storage.ShardNode, len(nodeShardsMapping))
	for {
		nodeNames := make([]string, 0, len(nodeShardsMapping))
		for nodeName := range nodeShardsMapping {
			nodeNames = append(nodeNames, nodeName)
		}

		// Reset node shards when shard is all picked.
		if len(result)%totalShardLength == 0 {
			for nodeName, shardNode := range nodeShardsMapping {
				tempShardNode := make([]storage.ShardNode, len(shardNode))
				copy(tempShardNode, shardNode)
				tempNodeShardMapping[nodeName] = tempShardNode
			}
		}

		for len(nodeNames) > 0 {
			if len(result) >= expectShardNum {
				return result, nil
			}

			selectNodeIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(nodeNames))))
			if err != nil {
				return nil, errors.WithMessage(err, "generate random node index")
			}

			nodeShards := tempNodeShardMapping[nodeNames[selectNodeIndex.Int64()]]

			if len(nodeShards) > 0 {
				result = append(result, nodeShards[0])

				// Remove select shard.
				nodeShards[0] = nodeShards[len(nodeShards)-1]
				tempNodeShardMapping[nodeNames[selectNodeIndex.Int64()]] = nodeShards[:len(nodeShards)-1]
			}

			// Remove select node.
			nodeNames[selectNodeIndex.Int64()] = nodeNames[len(nodeNames)-1]
			nodeNames = nodeNames[:len(nodeNames)-1]
		}
	}
}
