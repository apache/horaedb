// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

const (
	nodeLength            = 3
	selectOnlineNodeIndex = 1
)

func TestRandomNodePicker(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	nodePicker := NewRandomNodePicker()

	var nodes []metadata.RegisteredNode
	_, err := nodePicker.PickNode(ctx, nodes)
	re.Error(err)

	for i := 0; i < nodeLength; i++ {
		nodes = append(nodes, metadata.RegisteredNode{
			Node:       storage.Node{Name: strconv.Itoa(i), LastTouchTime: generateLastTouchTime(time.Minute)},
			ShardInfos: nil,
		})
	}
	_, err = nodePicker.PickNode(ctx, nodes)
	re.Error(err)

	nodes = nodes[:0]
	for i := 0; i < nodeLength; i++ {
		nodes = append(nodes, metadata.RegisteredNode{
			Node:       storage.Node{Name: strconv.Itoa(i), LastTouchTime: generateLastTouchTime(0)},
			ShardInfos: nil,
		})
	}
	_, err = nodePicker.PickNode(ctx, nodes)
	re.NoError(err)

	nodes = nodes[:0]
	for i := 0; i < nodeLength; i++ {
		nodes = append(nodes, metadata.RegisteredNode{
			Node:       storage.Node{Name: strconv.Itoa(i), LastTouchTime: generateLastTouchTime(time.Minute)},
			ShardInfos: nil,
		})
	}
	nodes[selectOnlineNodeIndex].Node.LastTouchTime = uint64(time.Now().Unix())
	node, err := nodePicker.PickNode(ctx, nodes)
	re.NoError(err)
	re.Equal(strconv.Itoa(selectOnlineNodeIndex), node.Node.Name)
}

func generateLastTouchTime(duration time.Duration) uint64 {
	return uint64(time.Now().Unix() - int64(duration))
}
