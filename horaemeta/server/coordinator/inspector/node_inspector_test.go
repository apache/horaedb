/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package inspector

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/LeslieKid/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/LeslieKid/incubator-horaedb-meta/server/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type mockClusterMetaDataManipulator struct {
	snapshot          metadata.Snapshot
	lock              sync.Mutex
	droppedShardNodes [][]storage.ShardNode
}

func newMockClusterMetaDataManipulator(shardNodes []storage.ShardNode, registeredNodes []metadata.RegisteredNode) *mockClusterMetaDataManipulator {
	var clusterView storage.ClusterView
	clusterView.ShardNodes = shardNodes
	topology := metadata.Topology{
		ShardViewsMapping: nil,
		ClusterView:       clusterView,
	}

	snapshot := metadata.Snapshot{
		Topology:        topology,
		RegisteredNodes: registeredNodes,
	}
	return &mockClusterMetaDataManipulator{
		snapshot:          snapshot,
		lock:              sync.Mutex{},
		droppedShardNodes: make([][]storage.ShardNode, 0),
	}
}

func (n *mockClusterMetaDataManipulator) GetClusterSnapshot() metadata.Snapshot {
	return n.snapshot
}

func (n *mockClusterMetaDataManipulator) DropShardNodes(_ context.Context, shardNodes []storage.ShardNode) error {
	n.lock.Lock()
	defer n.lock.Unlock()

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

func (n *mockClusterMetaDataManipulator) CheckDroppedShardNodes(check func(droppedShardNodes [][]storage.ShardNode)) {
	n.lock.Lock()
	defer n.lock.Unlock()

	check(n.droppedShardNodes)
}

func TestStartStopInspector(t *testing.T) {
	inspector := NewNodeInspector(zap.NewNop(), newMockClusterMetaDataManipulator(nil, nil))

	ctx := context.Background()
	assert.NoError(t, inspector.Start(ctx))
	assert.Error(t, inspector.Start(ctx))

	assert.NoError(t, inspector.Stop(ctx))
}

func TestInspect(t *testing.T) {
	shardNodes := []storage.ShardNode{
		{ID: storage.ShardID(0), ShardRole: storage.ShardRoleLeader, NodeName: "192.168.1.102"},
		{ID: storage.ShardID(1), ShardRole: storage.ShardRoleLeader, NodeName: "192.168.1.102"},
		{ID: storage.ShardID(2), ShardRole: storage.ShardRoleLeader, NodeName: "192.168.1.103"},
		{ID: storage.ShardID(3), ShardRole: storage.ShardRoleLeader, NodeName: "192.168.1.103"},
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
			// This node should be outdated.
			Node: storage.Node{
				Name:          "192.168.1.103",
				NodeStats:     storage.NodeStats{Lease: 0, Zone: "", NodeVersion: ""},
				LastTouchTime: uint64(time.Now().UnixMilli()) - uint64((time.Second * 20)),
				State:         storage.NodeStateOnline,
			},
			ShardInfos: nil,
		},
	}

	metadata := newMockClusterMetaDataManipulator(shardNodes, registeredNodes)
	inspector := NewNodeInspectorWithInterval(zap.NewNop(), metadata, time.Millisecond*100)
	ctx := context.Background()
	assert.NoError(t, inspector.Start(ctx))

	// The inspect should be triggered after 200ms.
	time.Sleep(time.Millisecond * 200)

	// The outdated node should be removed by triggered.
	metadata.CheckDroppedShardNodes(func(droppedShardNodes [][]storage.ShardNode) {
		assert.True(t, len(droppedShardNodes) == 1)
		assert.True(t, len(droppedShardNodes[0]) == 2)
		assert.Equal(t, droppedShardNodes[0][0], shardNodes[2])
		assert.Equal(t, droppedShardNodes[0][1], shardNodes[3])
	})

	assert.NoError(t, inspector.Stop(ctx))
}
