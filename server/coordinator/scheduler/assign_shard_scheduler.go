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
)

// AssignShardScheduler used to assigning shards without nodes.
type AssignShardScheduler struct {
	factory                     *coordinator.Factory
	nodePicker                  coordinator.NodePicker
	procedureExecutingBatchSize uint32
}

func NewAssignShardScheduler(factory *coordinator.Factory, nodePicker coordinator.NodePicker, procedureExecutingBatchSize uint32) Scheduler {
	return &AssignShardScheduler{
		factory:                     factory,
		nodePicker:                  nodePicker,
		procedureExecutingBatchSize: procedureExecutingBatchSize,
	}
}

func (a AssignShardScheduler) Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (ScheduleResult, error) {
	if clusterSnapshot.Topology.ClusterView.State == storage.ClusterStateEmpty {
		return ScheduleResult{}, nil
	}

	var procedures []procedure.Procedure
	var reasons strings.Builder
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

		procedures = append(procedures, p)
		reasons.WriteString(fmt.Sprintf("the shard is not assigned to any node, try to assign it to node, shardID:%d, node:%s.", shardView.ShardID, newLeaderNode.Node.Name))
		if len(procedures) >= int(a.procedureExecutingBatchSize) {
			break
		}
	}

	if len(procedures) == 0 {
		return ScheduleResult{}, nil
	}

	batchProcedure, err := a.factory.CreateBatchTransferLeaderProcedure(ctx, coordinator.BatchRequest{
		Batch:     procedures,
		BatchType: procedure.TransferLeader,
	})
	if err != nil {
		return ScheduleResult{}, err
	}

	return ScheduleResult{
		batchProcedure,
		reasons.String(),
	}, nil
}

func findNodeByShard(shardID storage.ShardID, shardNodes []storage.ShardNode) (storage.ShardNode, bool) {
	for i := 0; i < len(shardNodes); i++ {
		if shardID == shardNodes[i].ID {
			return shardNodes[i], true
		}
	}
	return storage.ShardNode{}, false
}
