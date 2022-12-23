// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRandomShardPicker(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	manager, _ := prepare(t)

	randomShardPicker := NewRandomShardPicker(manager)
	nodeShards, err := randomShardPicker.PickShards(ctx, clusterName, 2)
	re.NoError(err)

	// Verify the number of shards and ensure that they are not on the same node.
	re.Equal(len(nodeShards), 2)
	re.NotEqual(nodeShards[0].ShardNode.NodeName, nodeShards[1].ShardNode.NodeName)
}
