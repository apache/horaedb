// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
)

// ReopenShardScheduler used to reopen shards in status PartitionOpen.
type ReopenShardScheduler struct {
	factory                     *coordinator.Factory
	procedureExecutingBatchSize uint32
}

func NewReopenShardScheduler(factory *coordinator.Factory, procedureExecutingBatchSize uint32) ReopenShardScheduler {
	return ReopenShardScheduler{
		factory:                     factory,
		procedureExecutingBatchSize: procedureExecutingBatchSize,
	}
}

func (r ReopenShardScheduler) Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (ScheduleResult, error) {
	// ReopenShardScheduler can only be scheduled when the cluster is stable.
	if !clusterSnapshot.Topology.IsStable() {
		return ScheduleResult{}, nil
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
				return ScheduleResult{}, err
			}

			procedures = append(procedures, p)
			reasons.WriteString(fmt.Sprintf("the shard needs to be reopen , shardID:%d, shardStatus:%d, node:%s.", shardInfo.ID, shardInfo.Status, registeredNode.Node.Name))
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

	return ScheduleResult{
		batchProcedure,
		reasons.String(),
	}, nil
}

func needReopen(shardInfo metadata.ShardInfo) bool {
	return shardInfo.Status == storage.ShardStatusPartialOpen
}
