// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package reopen

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/scheduler"
	"github.com/CeresDB/ceresmeta/server/storage"
)

// schedulerImpl used to reopen shards in status PartitionOpen.
type schedulerImpl struct {
	factory                     *coordinator.Factory
	procedureExecutingBatchSize uint32
}

func NewShardScheduler(factory *coordinator.Factory, procedureExecutingBatchSize uint32) scheduler.Scheduler {
	return schedulerImpl{
		factory:                     factory,
		procedureExecutingBatchSize: procedureExecutingBatchSize,
	}
}

func (r schedulerImpl) Name() string {
	return "reopen_scheduler"
}

func (r schedulerImpl) UpdateDeployMode(_ context.Context, _ bool) {
	// ReopenShardScheduler do not need deployMode.
}

func (r schedulerImpl) AddShardAffinityRule(_ context.Context, _ scheduler.ShardAffinityRule) error {
	return nil
}

func (r schedulerImpl) RemoveShardAffinityRule(_ context.Context, _ storage.ShardID) error {
	return nil
}

func (r schedulerImpl) ListShardAffinityRule(_ context.Context) (scheduler.ShardAffinityRule, error) {
	return scheduler.ShardAffinityRule{Affinities: []scheduler.ShardAffinity{}}, nil
}

func (r schedulerImpl) Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (scheduler.ScheduleResult, error) {
	// ReopenShardScheduler can only be scheduled when the cluster is stable.
	if !clusterSnapshot.Topology.IsStable() {
		return scheduler.ScheduleResult{}, nil
	}
	now := time.Now()

	var procedures []procedure.Procedure
	var reasons strings.Builder

	for _, registeredNode := range clusterSnapshot.RegisteredNodes {
		if registeredNode.IsExpired(now) {
			continue
		}

		for _, shardInfo := range registeredNode.ShardInfos {
			if !needReopen(shardInfo) {
				continue
			}
			p, err := r.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
				Snapshot:          clusterSnapshot,
				ShardID:           shardInfo.ID,
				OldLeaderNodeName: "",
				NewLeaderNodeName: registeredNode.Node.Name,
			})
			if err != nil {
				return scheduler.ScheduleResult{}, err
			}

			procedures = append(procedures, p)
			reasons.WriteString(fmt.Sprintf("the shard needs to be reopen , shardID:%d, shardStatus:%d, node:%s.", shardInfo.ID, shardInfo.Status, registeredNode.Node.Name))
			if len(procedures) >= int(r.procedureExecutingBatchSize) {
				break
			}
		}
	}

	if len(procedures) == 0 {
		return scheduler.ScheduleResult{}, nil
	}

	batchProcedure, err := r.factory.CreateBatchTransferLeaderProcedure(ctx, coordinator.BatchRequest{
		Batch:     procedures,
		BatchType: procedure.TransferLeader,
	})
	if err != nil {
		return scheduler.ScheduleResult{}, err
	}

	return scheduler.ScheduleResult{
		Procedure: batchProcedure,
		Reason:    reasons.String(),
	}, nil
}

func needReopen(shardInfo metadata.ShardInfo) bool {
	return shardInfo.Status == storage.ShardStatusPartialOpen
}
