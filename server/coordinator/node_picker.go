// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"crypto/rand"
	"math/big"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/pkg/errors"
)

type NodePicker interface {
	PickNode(ctx context.Context, registerNodes []cluster.RegisteredNode) (cluster.RegisteredNode, error)
}

type RandomNodePicker struct{}

func NewRandomNodePicker() NodePicker {
	return &RandomNodePicker{}
}

func (p *RandomNodePicker) PickNode(_ context.Context, registeredNodes []cluster.RegisteredNode) (cluster.RegisteredNode, error) {
	onlineNodeLength := 0
	for _, registeredNode := range registeredNodes {
		if registeredNode.IsOnline() {
			onlineNodeLength++
		}
	}

	if onlineNodeLength == 0 {
		return cluster.RegisteredNode{}, errors.WithMessage(ErrNodeNumberNotEnough, "online node length must bigger than 0")
	}

	randSelectedIdx, err := rand.Int(rand.Reader, big.NewInt(int64(onlineNodeLength)))
	if err != nil {
		return cluster.RegisteredNode{}, errors.WithMessage(err, "generate random node index")
	}
	selectIdx := int(randSelectedIdx.Int64())
	curOnlineIdx := -1
	for idx := 0; idx < len(registeredNodes); idx++ {
		if registeredNodes[idx].IsOnline() {
			curOnlineIdx++
		}
		if curOnlineIdx == selectIdx {
			return registeredNodes[idx], nil
		}
	}

	return cluster.RegisteredNode{}, errors.WithMessage(ErrPickNode, "pick node failed")
}
