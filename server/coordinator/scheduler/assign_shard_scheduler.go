// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
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

func (a AssignShardScheduler) Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (ScheduleResult, error) {
	if clusterSnapshot.Topology.ClusterView.State != storage.ClusterStateStable {
		return ScheduleResult{}, nil
	}

	// Check whether there is a shard without node mapping.
	for _, shardView := range clusterSnapshot.Topology.ShardViewsMapping {
		_, exists := findNodeByShard(shardView.ShardID, clusterSnapshot.Topology.ClusterView.ShardNodes)
		if exists {
			continue
		}
		newLeaderNode, err := a.nodePicker.PickNode(ctx, shardView.ShardID, clusterSnapshot.RegisteredNodes)
		if err != nil {
			return ScheduleResult{}, err
		}
		// Shard exists and ShardNode not exists.
		p, err := a.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
			Snapshot:          clusterSnapshot,
			ShardID:           shardView.ShardID,
			OldLeaderNodeName: "",
			NewLeaderNodeName: newLeaderNode.Node.Name,
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
