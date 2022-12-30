// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/pkg/errors"
)

// ShardPicker is used to pick up the shards suitable for scheduling in the cluster.
// If expectShardNum bigger than cluster node number, the result depends on enableDuplicateNode:
// If enableDuplicateNode is false, pick shards will be failed and return error.
// If enableDuplicateNode is true, pick shard will return shards on the same node.
// TODO: Consider refactor this interface, abstracts the parameters of PickShards as PickStrategy.
type ShardPicker interface {
	PickShards(ctx context.Context, clusterName string, expectShardNum int, enableDuplicateNode bool) ([]cluster.ShardNodeWithVersion, error)
}

// RandomShardPicker randomly pick up shards that are not on the same node in the current cluster.
type RandomShardPicker struct {
	clusterManager cluster.Manager
}

func NewRandomShardPicker(manager cluster.Manager) ShardPicker {
	return &RandomShardPicker{
		clusterManager: manager,
	}
}

// PickShards will pick a specified number of shards as expectShardNum.
func (p *RandomShardPicker) PickShards(ctx context.Context, clusterName string, expectShardNum int, enableDuplicateNode bool) ([]cluster.ShardNodeWithVersion, error) {
	getNodeShardResult, err := p.clusterManager.GetNodeShards(ctx, clusterName)
	if err != nil {
		return []cluster.ShardNodeWithVersion{}, errors.WithMessage(err, "get node shards")
	}

	if expectShardNum > len(getNodeShardResult.NodeShards) {
		return []cluster.ShardNodeWithVersion{}, errors.WithMessage(ErrShardNumberNotEnough, fmt.Sprintf("number of shards is:%d, expecet number of shards is:%d", len(getNodeShardResult.NodeShards), expectShardNum))
	}

	nodeShardsMapping := make(map[string][]cluster.ShardNodeWithVersion, 0)
	for _, nodeShard := range getNodeShardResult.NodeShards {
		_, exists := nodeShardsMapping[nodeShard.ShardNode.NodeName]
		if !exists {
			nodeShards := []cluster.ShardNodeWithVersion{}
			nodeShardsMapping[nodeShard.ShardNode.NodeName] = nodeShards
		}
		nodeShardsMapping[nodeShard.ShardNode.NodeName] = append(nodeShardsMapping[nodeShard.ShardNode.NodeName], nodeShard)
	}

	if !enableDuplicateNode {
		if len(nodeShardsMapping) < expectShardNum {
			return []cluster.ShardNodeWithVersion{}, errors.WithMessage(ErrNodeNumberNotEnough, fmt.Sprintf("number of nodes is:%d, expecet number of shards is:%d", len(nodeShardsMapping), expectShardNum))
		}
	}

	// Try to make shards on different nodes.
	result := []cluster.ShardNodeWithVersion{}
	for {
		nodeNames := []string{}
		for nodeName := range nodeShardsMapping {
			nodeNames = append(nodeNames, nodeName)
		}

		for len(nodeNames) > 0 {
			if len(result) >= expectShardNum {
				return result, nil
			}

			selectNodeIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(nodeNames))))
			if err != nil {
				return []cluster.ShardNodeWithVersion{}, errors.WithMessage(err, "generate random node index")
			}

			nodeShards := nodeShardsMapping[nodeNames[selectNodeIndex.Int64()]]

			if len(nodeShards) > 0 {
				result = append(result, nodeShards[0])

				// Remove select shard.
				nodeShards[0] = nodeShards[len(nodeShards)-1]
				nodeShardsMapping[nodeNames[selectNodeIndex.Int64()]] = nodeShards[:len(nodeShards)-1]
			}

			// Remove select node.
			nodeNames[selectNodeIndex.Int64()] = nodeNames[len(nodeNames)-1]
			nodeNames = nodeNames[:len(nodeNames)-1]
		}
	}
}
