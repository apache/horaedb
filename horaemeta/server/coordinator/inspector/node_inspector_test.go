package inspector

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type mockClusterMetaDataManipulator struct {
	snapshot          metadata.Snapshot
	droppedShardNodes [][]storage.ShardNode
}

func newMockClusterMetaDataManipulator() ClusterMetaDataManipulator {
	var clusterView storage.ClusterView
	clusterView.ShardNodes = []storage.ShardNode{
		{ID: storage.ShardID(0), ShardRole: storage.ShardRoleLeader, NodeName: "192.168.1.102"},
		{ID: storage.ShardID(1), ShardRole: storage.ShardRoleLeader, NodeName: "192.168.1.102"},
		{ID: storage.ShardID(2), ShardRole: storage.ShardRoleLeader, NodeName: "192.168.1.103"},
		{ID: storage.ShardID(3), ShardRole: storage.ShardRoleLeader, NodeName: "192.168.1.104"},
	}

	topology := metadata.Topology{
		ShardViewsMapping: nil,
		ClusterView:       clusterView,
	}
	registeredNodes := []metadata.RegisteredNode{
		{
			Node: storage.Node{
				Name:          "192.168.1.102",
				NodeStats:     storage.NodeStats{Lease: 0, Zone: "", NodeVersion: ""},
				LastTouchTime: uint64(time.Now().UnixMilli()),
				State:         storage.NodeStateOnline,
			},
			ShardInfos: nil,
		},
		{
			Node: storage.Node{
				Name:          "192.168.1.103",
				NodeStats:     storage.NodeStats{Lease: 0, Zone: "", NodeVersion: ""},
				LastTouchTime: uint64(time.Now().UnixMilli()) - uint64((time.Second * 20)),
				State:         storage.NodeStateOnline,
			},
			ShardInfos: nil,
		},
	}

	snapshot := metadata.Snapshot{
		Topology:        topology,
		RegisteredNodes: registeredNodes,
	}
	return &mockClusterMetaDataManipulator{
		snapshot:          snapshot,
		droppedShardNodes: make([][]storage.ShardNode, 0),
	}
}

func (n *mockClusterMetaDataManipulator) GetClusterSnapshot() metadata.Snapshot {
	return n.snapshot
}

func (n *mockClusterMetaDataManipulator) DropShardNode(_ context.Context, shardNodes []storage.ShardNode) error {
	n.droppedShardNodes = append(n.droppedShardNodes, shardNodes)
	newShardNodes := make([]storage.ShardNode, 0, 2)
	for _, node := range n.snapshot.Topology.ClusterView.ShardNodes {
		dropped := slices.ContainsFunc(shardNodes, func(droppedNode storage.ShardNode) bool {
			return node.NodeName == droppedNode.NodeName
		})
		if !dropped {
			newShardNodes = append(newShardNodes, node)
		}
	}
	n.snapshot.Topology.ClusterView.ShardNodes = newShardNodes
	return nil
}

func TestStartStopInspector(t *testing.T) {
	inspector := NewNodeInspector(zap.NewNop(), newMockClusterMetaDataManipulator())

	ctx := context.Background()
	assert.NoError(t, inspector.Start(ctx))
	assert.Error(t, inspector.Start(ctx))

	assert.NoError(t, inspector.Stop(ctx))
}

func TestInspect(t *testing.T) {
	// TODO: add a integrate inspect test case
}
