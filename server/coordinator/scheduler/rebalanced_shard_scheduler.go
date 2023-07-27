// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"
	"fmt"
	"strings"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"go.uber.org/zap"
)

type RebalancedShardScheduler struct {
	logger                      *zap.Logger
	factory                     *coordinator.Factory
	nodePicker                  coordinator.NodePicker
	procedureExecutingBatchSize uint32
}

func NewRebalancedShardScheduler(logger *zap.Logger, factory *coordinator.Factory, nodePicker coordinator.NodePicker, procedureExecutingBatchSize uint32) Scheduler {
	return &RebalancedShardScheduler{
		logger:                      logger,
		factory:                     factory,
		nodePicker:                  nodePicker,
		procedureExecutingBatchSize: procedureExecutingBatchSize,
	}
}

func (r RebalancedShardScheduler) Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (ScheduleResult, error) {
	// RebalancedShardScheduler can only be scheduled when the cluster is stable.
	if !clusterSnapshot.Topology.IsStable() {
		return ScheduleResult{}, nil
	}

	var procedures []procedure.Procedure
	var reasons strings.Builder
	// TODO: Improve scheduling efficiency and verify whether the topology changes.
	shardIDs := make([]storage.ShardID, 0, len(clusterSnapshot.Topology.ShardViewsMapping))
	for shardID := range clusterSnapshot.Topology.ShardViewsMapping {
		shardIDs = append(shardIDs, shardID)
	}
	shardNodeMapping, err := r.nodePicker.PickNode(ctx, shardIDs, uint32(len(clusterSnapshot.Topology.ShardViewsMapping)), clusterSnapshot.RegisteredNodes)
	if err != nil {
		return ScheduleResult{}, err
	}

	for _, shardNode := range clusterSnapshot.Topology.ClusterView.ShardNodes {
		newLeaderNode := shardNodeMapping[shardNode.ID]
		if newLeaderNode.Node.Name != shardNode.NodeName {
			r.logger.Info("rebalanced shard scheduler generate new procedure", zap.Uint64("shardID", uint64(shardNode.ID)), zap.String("originNode", shardNode.NodeName), zap.String("newNode", newLeaderNode.Node.Name))
			p, err := r.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
				Snapshot:          clusterSnapshot,
				ShardID:           shardNode.ID,
				OldLeaderNodeName: shardNode.NodeName,
				NewLeaderNodeName: newLeaderNode.Node.Name,
			})
			if err != nil {
				return ScheduleResult{}, err
			}
			procedures = append(procedures, p)
			reasons.WriteString(fmt.Sprintf("the shard does not meet the balance requirements,it should be assigned to node, shardID:%d, oldNode:%s, newNode:%s.", shardNode.ID, shardNode.NodeName, newLeaderNode.Node.Name))
			if len(procedures) >= int(r.procedureExecutingBatchSize) {
				break
			}
		}
	}

	if len(procedures) == 0 {
		return ScheduleResult{}, nil
	}

	batchProcedure, err := r.factory.CreateBatchTransferLeaderProcedure(ctx, coordinator.BatchRequest{
		Batch:     procedures,
		BatchType: procedure.TransferLeader,
	})
	if err != nil {
		return ScheduleResult{}, err
	}

	return ScheduleResult{batchProcedure, reasons.String()}, nil
}
