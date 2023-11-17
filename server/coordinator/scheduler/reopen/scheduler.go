/*
 * Copyright 2022 The CeresDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reopen

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/CeresDB/horaemeta/server/cluster/metadata"
	"github.com/CeresDB/horaemeta/server/coordinator"
	"github.com/CeresDB/horaemeta/server/coordinator/procedure"
	"github.com/CeresDB/horaemeta/server/coordinator/scheduler"
	"github.com/CeresDB/horaemeta/server/storage"
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

func (r schedulerImpl) UpdateEnableSchedule(_ context.Context, _ bool) {
	// ReopenShardScheduler do not need enableSchedule.
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
	var scheduleRes scheduler.ScheduleResult
	// ReopenShardScheduler can only be scheduled when the cluster is stable.
	if !clusterSnapshot.Topology.IsStable() {
		return scheduleRes, nil
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
				return scheduleRes, err
			}

			procedures = append(procedures, p)
			reasons.WriteString(fmt.Sprintf("the shard needs to be reopen , shardID:%d, shardStatus:%d, node:%s.", shardInfo.ID, shardInfo.Status, registeredNode.Node.Name))
			if len(procedures) >= int(r.procedureExecutingBatchSize) {
				break
			}
		}
	}

	if len(procedures) == 0 {
		return scheduleRes, nil
	}

	batchProcedure, err := r.factory.CreateBatchTransferLeaderProcedure(ctx, coordinator.BatchRequest{
		Batch:     procedures,
		BatchType: procedure.TransferLeader,
	})
	if err != nil {
		return scheduleRes, err
	}

	scheduleRes = scheduler.ScheduleResult{
		Procedure: batchProcedure,
		Reason:    reasons.String(),
	}
	return scheduleRes, nil
}

func needReopen(shardInfo metadata.ShardInfo) bool {
	return shardInfo.Status == storage.ShardStatusPartialOpen
}
