// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/stretchr/testify/require"
)

func newClusterAndRegisterNode(t *testing.T) *cluster.Cluster {
	re := require.New(t)
	ctx := context.Background()
	dispatch := MockDispatch{}
	cluster := newTestCluster(ctx, t)

	nodeInfo1 := &metaservicepb.NodeInfo{
		Endpoint:   nodeName0,
		ShardInfos: nil,
	}

	totalShardNum := cluster.GetTotalShardNum()
	shardIDs := make([]uint32, 0, totalShardNum)
	for i := uint32(0); i < totalShardNum; i++ {
		shardIDs = append(shardIDs, i)
	}
	p := NewScatterProcedure(dispatch, cluster, 1, shardIDs)
	go func() {
		err := p.Start(ctx)
		re.NoError(err)
	}()

	// Cluster is empty, it should be return and do nothing
	err := cluster.RegisterNode(ctx, nodeInfo1)
	re.NoError(err)
	re.Equal(clusterpb.ClusterTopology_EMPTY, cluster.GetClusterState())

	// Register two node, defaultNodeCount is satisfied, Initialize shard topology
	nodeInfo2 := &metaservicepb.NodeInfo{
		Endpoint:   nodeName1,
		ShardInfos: nil,
	}
	err = cluster.RegisterNode(ctx, nodeInfo2)
	re.NoError(err)
	return cluster
}

func checkScatterWithCluster(t *testing.T, cluster *cluster.Cluster) {
	re := require.New(t)
	re.Equal(clusterpb.ClusterTopology_STABLE, cluster.GetClusterState())
	shardViews, err := cluster.GetClusterShardView()
	re.NoError(err)
	re.Equal(len(shardViews), defaultShardTotal)
	shardNodeMapping := make(map[string][]uint32, 0)
	for _, shardView := range shardViews {
		nodeName := shardView.GetNode()
		shardID := shardView.GetId()
		_, exists := shardNodeMapping[nodeName]
		if !exists {
			shardNodeMapping[nodeName] = make([]uint32, 0)
		}
		shardNodeMapping[nodeName] = append(shardNodeMapping[nodeName], shardID)
	}
	// Cluster shard topology should be initialized, shard length in every node should be defaultShardTotal/defaultNodeCount
	re.Equal(len(shardNodeMapping), defaultNodeCount)
	re.Equal(len(shardNodeMapping[nodeName0]), defaultShardTotal/defaultNodeCount)
	re.Equal(len(shardNodeMapping[nodeName1]), defaultShardTotal/defaultNodeCount)
}

func TestScatter(t *testing.T) {
	cluster := newClusterAndRegisterNode(t)
	time.Sleep(time.Second * 5)
	checkScatterWithCluster(t, cluster)
}

func TestAllocNodeShard(t *testing.T) {
	re := require.New(t)

	minNodeCount := 4
	shardTotal := 2
	nodeList := make([]*cluster.RegisteredNode, 0, minNodeCount)
	for i := 0; i < minNodeCount; i++ {
		nodeMeta := &clusterpb.Node{
			Name: fmt.Sprintf("node%d", i),
		}
		node := cluster.NewRegisteredNode(nodeMeta, []*cluster.ShardInfo{})
		nodeList = append(nodeList, node)
	}
	shardIDs := make([]uint32, 0, shardTotal)
	for i := uint32(0); i < uint32(shardTotal); i++ {
		shardIDs = append(shardIDs, i)
	}
	// NodeCount = 4, shardTotal = 2
	// Two shard distributed in node0,node1
	shardView, err := allocNodeShards(uint32(shardTotal), uint32(minNodeCount), nodeList, shardIDs)
	re.NoError(err)
	re.Equal(shardTotal, len(shardView))
	re.Equal("node0", shardView[0].Node)
	re.Equal("node1", shardView[1].Node)

	minNodeCount = 2
	shardTotal = 3
	nodeList = make([]*cluster.RegisteredNode, 0, minNodeCount)
	for i := 0; i < minNodeCount; i++ {
		nodeMeta := &clusterpb.Node{
			Name: fmt.Sprintf("node%d", i),
		}
		node := cluster.NewRegisteredNode(nodeMeta, []*cluster.ShardInfo{})
		nodeList = append(nodeList, node)
	}
	// NodeCount = 2, shardTotal = 3
	// Three shard distributed in node0,node0,node1
	shardIDs = make([]uint32, 0)
	for i := uint32(0); i < uint32(shardTotal); i++ {
		shardIDs = append(shardIDs, i)
	}
	shardView, err = allocNodeShards(uint32(shardTotal), uint32(minNodeCount), nodeList, shardIDs)
	re.NoError(err)
	re.Equal(shardTotal, len(shardView))
	re.Equal("node0", shardView[0].Node)
	re.Equal("node0", shardView[1].Node)
	re.Equal("node1", shardView[2].Node)
}
