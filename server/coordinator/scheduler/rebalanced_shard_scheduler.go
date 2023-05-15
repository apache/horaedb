// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"
	"fmt"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"go.uber.org/zap"
)

type RebalancedShardScheduler struct {
	logger     *zap.Logger
	factory    *coordinator.Factory
	nodePicker coordinator.NodePicker
}

func NewRebalancedShardScheduler(logger *zap.Logger, factory *coordinator.Factory, nodePicker coordinator.NodePicker) Scheduler {
	return &RebalancedShardScheduler{
		logger:     logger,
		factory:    factory,
		nodePicker: nodePicker,
	}
}

func (r RebalancedShardScheduler) Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (ScheduleResult, error) {
	// RebalancedShardScheduler can only be scheduled when the cluster is stable.
	if !clusterSnapshot.Topology.IsStable() {
		return ScheduleResult{}, nil
	}

	// TODO: Improve scheduling efficiency and verify whether the topology changes.
	for _, shardNode := range clusterSnapshot.Topology.ClusterView.ShardNodes {
		node, err := r.nodePicker.PickNode(ctx, shardNode.ID, clusterSnapshot.RegisteredNodes)
		if err != nil {
			return ScheduleResult{}, err
		}
		if node.Node.Name != shardNode.NodeName {
			r.logger.Info("rebalanced shard scheduler generate new procedure", zap.Uint64("shardID", uint64(shardNode.ID)), zap.String("originNode", shardNode.NodeName), zap.String("newNode", node.Node.Name))
			p, err := r.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
				Snapshot:          clusterSnapshot,
				ShardID:           shardNode.ID,
				OldLeaderNodeName: shardNode.NodeName,
				NewLeaderNodeName: node.Node.Name,
			})
			if err != nil {
				return ScheduleResult{}, err
			}
			return ScheduleResult{
				Procedure: p,
				Reason:    fmt.Sprintf("the shard:%d on the node:%s does not meet the balance requirements,it should be assigned to node:%s", shardNode.ID, shardNode.NodeName, node.Node.Name),
			}, nil
		}
	}

	return ScheduleResult{}, nil
}
