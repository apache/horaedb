// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"strconv"
	"testing"

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
			Node:       storage.Node{Name: strconv.Itoa(i), State: storage.NodeStateOffline},
			ShardInfos: nil,
		})
	}
	_, err = nodePicker.PickNode(ctx, nodes)
	re.Error(err)

	nodes = nodes[:0]
	for i := 0; i < nodeLength; i++ {
		nodes = append(nodes, metadata.RegisteredNode{
			Node:       storage.Node{Name: strconv.Itoa(i), State: storage.NodeStateOnline},
			ShardInfos: nil,
		})
	}
	_, err = nodePicker.PickNode(ctx, nodes)
	re.NoError(err)

	nodes = nodes[:0]
	for i := 0; i < nodeLength; i++ {
		nodes = append(nodes, metadata.RegisteredNode{
			Node:       storage.Node{Name: strconv.Itoa(i), State: storage.NodeStateOffline},
			ShardInfos: nil,
		})
	}
	nodes[selectOnlineNodeIndex].Node.State = storage.NodeStateOnline
	node, err := nodePicker.PickNode(ctx, nodes)
	re.NoError(err)
	re.Equal(strconv.Itoa(selectOnlineNodeIndex), node.Node.Name)
}
