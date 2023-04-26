// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"strconv"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/hash"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type NodePicker interface {
	PickNode(ctx context.Context, shardID storage.ShardID, registerNodes []metadata.RegisteredNode) (metadata.RegisteredNode, error)
}

type ConsistentHashNodePicker struct {
	hashReplicas int
}

func NewConsistentHashNodePicker(hashReplicas int) NodePicker {
	return &ConsistentHashNodePicker{hashReplicas: hashReplicas}
}

func (p *ConsistentHashNodePicker) PickNode(_ context.Context, shardID storage.ShardID, registerNodes []metadata.RegisteredNode) (metadata.RegisteredNode, error) {
	now := time.Now()

	hashRing := hash.New(p.hashReplicas, nil)
	aliveNodeNumber := 0
	for _, registerNode := range registerNodes {
		if !registerNode.IsExpired(now) {
			hashRing.Add(registerNode.Node.Name)
			aliveNodeNumber++
		}
	}

	if hashRing.IsEmpty() {
		return metadata.RegisteredNode{}, errors.WithMessage(ErrNodeNumberNotEnough, "at least one online nodes is required")
	}

	pickedNodeName := hashRing.Get(strconv.Itoa(int(shardID)))
	log.Debug("ConsistentHashNodePicker pick result", zap.Uint64("shardID", uint64(shardID)), zap.String("node", pickedNodeName), zap.Int("nodeNumber", aliveNodeNumber))

	for i := 0; i < len(registerNodes); i++ {
		if registerNodes[i].Node.Name == pickedNodeName {
			return registerNodes[i], nil
		}
	}

	return metadata.RegisteredNode{}, errors.WithMessagef(ErrPickNode, "pickedNode not found in register nodes, nodeName:%s", pickedNodeName)
}
