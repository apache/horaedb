// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scatter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

func checkScatterWithCluster(t *testing.T, cluster *cluster.Cluster) {
	re := require.New(t)
	re.Equal(storage.ClusterStateStable, cluster.GetClusterState())
	shardNodes, err := cluster.GetNodeShards(context.Background())
	re.NoError(err)
	re.Equal(len(shardNodes.NodeShards), test.DefaultShardTotal)
	shardNodeMapping := make(map[string][]storage.ShardID, 0)
	for _, shardNodeWithVersion := range shardNodes.NodeShards {
		nodeName := shardNodeWithVersion.ShardNode.NodeName
		shardID := shardNodeWithVersion.ShardNode.ID
		_, exists := shardNodeMapping[nodeName]
		if !exists {
			shardNodeMapping[nodeName] = make([]storage.ShardID, 0)
		}
		shardNodeMapping[nodeName] = append(shardNodeMapping[nodeName], shardID)
	}
	// Cluster shard topology should be initialized, shard length in every node should be DefaultShardTotal/DefaultNodeCount
	re.Equal(len(shardNodeMapping), test.DefaultNodeCount)
	re.Equal(len(shardNodeMapping[test.NodeName0]), test.DefaultShardTotal/test.DefaultNodeCount)
	re.Equal(len(shardNodeMapping[test.NodeName1]), test.DefaultShardTotal/test.DefaultNodeCount)
}

func TestScatter(t *testing.T) {
	_, cluster := newClusterAndRegisterNode(t)
	time.Sleep(time.Second * 5)
	checkScatterWithCluster(t, cluster)
}

func TestAllocNodeShard(t *testing.T) {
	re := require.New(t)

	minNodeCount := 4
	shardTotal := 2
	nodeList := make([]cluster.RegisteredNode, 0, minNodeCount)
	for i := 0; i < minNodeCount; i++ {
		nodeMeta := storage.Node{
			Name: fmt.Sprintf("node%d", i),
		}
		node := cluster.NewRegisteredNode(nodeMeta, []cluster.ShardInfo{})
		nodeList = append(nodeList, node)
	}
	shardIDs := make([]storage.ShardID, 0, shardTotal)
	for i := uint32(0); i < uint32(shardTotal); i++ {
		shardIDs = append(shardIDs, storage.ShardID(i))
	}
	// NodeCount = 4, shardTotal = 2
	// Two shard distributed in node0,node1
	shardView, err := AllocNodeShards(uint32(shardTotal), uint32(minNodeCount), nodeList, shardIDs)
	re.NoError(err)
	re.Equal(shardTotal, len(shardView))
	re.Equal("node0", shardView[0].NodeName)
	re.Equal("node1", shardView[1].NodeName)

	minNodeCount = 2
	shardTotal = 3
	nodeList = make([]cluster.RegisteredNode, 0, minNodeCount)
	for i := 0; i < minNodeCount; i++ {
		nodeMeta := storage.Node{
			Name: fmt.Sprintf("node%d", i),
		}
		node := cluster.NewRegisteredNode(nodeMeta, []cluster.ShardInfo{})
		nodeList = append(nodeList, node)
	}
	// NodeCount = 2, shardTotal = 3
	// Three shard distributed in node0,node0,node1
	shardIDs = make([]storage.ShardID, 0, shardTotal)
	for i := uint32(0); i < uint32(shardTotal); i++ {
		shardIDs = append(shardIDs, storage.ShardID(i))
	}
	shardView, err = AllocNodeShards(uint32(shardTotal), uint32(minNodeCount), nodeList, shardIDs)
	re.NoError(err)
	re.Equal(shardTotal, len(shardView))
	re.Equal("node0", shardView[0].NodeName)
	re.Equal("node0", shardView[1].NodeName)
	re.Equal("node1", shardView[2].NodeName)
}
