// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/storage"
)

const (
	AssignReason = "ShardView exists in metadata but shardNode not exists, assign shard to node"
)

// AssignShardScheduler used to assigning shards without nodes.
type AssignShardScheduler struct {
	factory    *coordinator.Factory
	nodePicker coordinator.NodePicker
}

func NewAssignShardScheduler(factory *coordinator.Factory, nodePicker coordinator.NodePicker) Scheduler {
	return &AssignShardScheduler{
		factory:    factory,
		nodePicker: nodePicker,
	}
}

func (a AssignShardScheduler) Schedule(ctx context.Context, clusterSnapshot cluster.Snapshot) (ScheduleResult, error) {
	if clusterSnapshot.Topology.ClusterView.State != storage.ClusterStateStable {
		return ScheduleResult{}, cluster.ErrClusterStateInvalid
	}

	// Check whether there is a shard without node mapping.
	for i := 0; i < len(clusterSnapshot.Topology.ShardViews); i++ {
		shardView := clusterSnapshot.Topology.ShardViews[i]
		_, exists := findNodeByShard(shardView.ShardID, clusterSnapshot.Topology.ClusterView.ShardNodes)
		if exists {
			continue
		}
		newLeaderNode, err := a.nodePicker.PickNode(ctx, clusterSnapshot.RegisteredNodes)
		if err != nil {
			return ScheduleResult{}, err
		}
		// Shard exists and ShardNode not exists.
		// TODO: ClusterName is no longer need in procedure, replace it with shardNodes.
		p, err := a.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
			ClusterName:       "",
			ShardID:           shardView.ShardID,
			OldLeaderNodeName: "",
			NewLeaderNodeName: newLeaderNode.Node.Name,
			ClusterVersion:    clusterSnapshot.Topology.ClusterView.Version,
		})
		if err != nil {
			return ScheduleResult{}, err
		}
		return ScheduleResult{
			Procedure: p,
			Reason:    AssignReason,
		}, nil
	}
	return ScheduleResult{}, nil
}

func findNodeByShard(shardID storage.ShardID, shardNodes []storage.ShardNode) (storage.ShardNode, bool) {
	for i := 0; i < len(shardNodes); i++ {
		if shardID == shardNodes[i].ID {
			return shardNodes[i], true
		}
	}
	return storage.ShardNode{}, false
}
