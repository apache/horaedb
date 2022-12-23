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
type ShardPicker interface {
	PickShards(ctx context.Context, clusterName string, expectShardNum int) ([]cluster.ShardNodeWithVersion, error)
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
func (p *RandomShardPicker) PickShards(ctx context.Context, clusterName string, expectShardNum int) ([]cluster.ShardNodeWithVersion, error) {
	getNodeShardResult, err := p.clusterManager.GetNodeShards(ctx, clusterName)
	if err != nil {
		return []cluster.ShardNodeWithVersion{}, errors.WithMessage(err, "get node shards")
	}
	nodeShardsMapping := make(map[string][]cluster.ShardNodeWithVersion, 0)
	var nodeNames []string
	for _, nodeShard := range getNodeShardResult.NodeShards {
		_, exists := nodeShardsMapping[nodeShard.ShardNode.NodeName]
		if !exists {
			nodeShards := []cluster.ShardNodeWithVersion{}
			nodeNames = append(nodeNames, nodeShard.ShardNode.NodeName)
			nodeShardsMapping[nodeShard.ShardNode.NodeName] = nodeShards
		}
		nodeShardsMapping[nodeShard.ShardNode.NodeName] = append(nodeShardsMapping[nodeShard.ShardNode.NodeName], nodeShard)
	}
	if len(nodeShardsMapping) < expectShardNum {
		return []cluster.ShardNodeWithVersion{}, errors.WithMessage(ErrNodeNumberNotEnough, fmt.Sprintf("number of nodes is:%d, expecet number of shards is:%d", len(nodeShardsMapping), expectShardNum))
	}

	var selectNodeNames []string
	for i := 0; i < expectShardNum; i++ {
		selectNodeIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(nodeNames))))
		if err != nil {
			return []cluster.ShardNodeWithVersion{}, errors.WithMessage(err, "generate random node index")
		}
		selectNodeNames = append(selectNodeNames, nodeNames[selectNodeIndex.Int64()])
		nodeNames[selectNodeIndex.Int64()] = nodeNames[len(nodeNames)-1]
		nodeNames = nodeNames[:len(nodeNames)-1]
	}

	result := []cluster.ShardNodeWithVersion{}
	for _, nodeName := range selectNodeNames {
		nodeShards := nodeShardsMapping[nodeName]
		selectShardIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(nodeShards))))
		if err != nil {
			return []cluster.ShardNodeWithVersion{}, errors.WithMessage(err, "generate random node index")
		}
		result = append(result, nodeShards[selectShardIndex.Int64()])
	}

	return result, nil
}
