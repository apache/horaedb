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

package rebalanced

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/assert"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/scheduler"
	"github.com/CeresDB/ceresmeta/server/coordinator/scheduler/nodepicker"
	"github.com/CeresDB/ceresmeta/server/storage"
	"go.uber.org/zap"
)

type schedulerImpl struct {
	logger                      *zap.Logger
	factory                     *coordinator.Factory
	nodePicker                  nodepicker.NodePicker
	procedureExecutingBatchSize uint32

	// The lock is used to protect following fields.
	lock sync.Mutex
	// latestShardNodeMapping is used to record last stable shard topology,
	// when deployMode is true, rebalancedShardScheduler will recover cluster according to the topology.
	latestShardNodeMapping map[storage.ShardID]metadata.RegisteredNode
	// The `latestShardNodeMapping` will be used directly, if deployMode is set.
	deployMode bool
	// shardAffinityRule is used to control the shard distribution.
	shardAffinityRule map[storage.ShardID]scheduler.ShardAffinity
}

func NewShardScheduler(logger *zap.Logger, factory *coordinator.Factory, nodePicker nodepicker.NodePicker, procedureExecutingBatchSize uint32) scheduler.Scheduler {
	return &schedulerImpl{
		logger:                      logger,
		factory:                     factory,
		nodePicker:                  nodePicker,
		procedureExecutingBatchSize: procedureExecutingBatchSize,
		lock:                        sync.Mutex{},
		latestShardNodeMapping:      map[storage.ShardID]metadata.RegisteredNode{},
		deployMode:                  false,
		shardAffinityRule:           map[storage.ShardID]scheduler.ShardAffinity{},
	}
}

func (r *schedulerImpl) Name() string {
	return "rebalanced_scheduler"
}

func (r *schedulerImpl) UpdateDeployMode(_ context.Context, enable bool) {
	r.updateDeployMode(enable)
}

func (r *schedulerImpl) AddShardAffinityRule(_ context.Context, rule scheduler.ShardAffinityRule) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, shardAffinity := range rule.Affinities {
		r.shardAffinityRule[shardAffinity.ShardID] = shardAffinity
	}

	return nil
}

func (r *schedulerImpl) RemoveShardAffinityRule(_ context.Context, shardID storage.ShardID) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	delete(r.shardAffinityRule, shardID)

	return nil
}

func (r *schedulerImpl) ListShardAffinityRule(_ context.Context) (scheduler.ShardAffinityRule, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	affinities := make([]scheduler.ShardAffinity, 0, len(r.shardAffinityRule))
	for _, affinity := range r.shardAffinityRule {
		affinities = append(affinities, affinity)
	}

	return scheduler.ShardAffinityRule{Affinities: affinities}, nil
}

func (r *schedulerImpl) Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (scheduler.ScheduleResult, error) {
	var emptySchedulerRes scheduler.ScheduleResult
	// RebalancedShardScheduler can only be scheduled when the cluster is not empty.
	if clusterSnapshot.Topology.ClusterView.State == storage.ClusterStateEmpty {
		return emptySchedulerRes, nil
	}

	var procedures []procedure.Procedure
	var reasons strings.Builder

	// ShardNodeMapping only update when deployMode is false.
	shardNodeMapping, err := r.generateLatestShardNodeMapping(ctx, clusterSnapshot)
	if err != nil {
		return emptySchedulerRes, nil
	}

	numShards := uint32(len(clusterSnapshot.Topology.ShardViewsMapping))
	// Generate assigned shards mapping and transfer leader if node is changed.
	assignedShardIDs := make(map[storage.ShardID]struct{}, numShards)
	for _, shardNode := range clusterSnapshot.Topology.ClusterView.ShardNodes {
		if len(procedures) >= int(r.procedureExecutingBatchSize) {
			r.logger.Warn("procedure length reached procedure executing batch size", zap.Uint32("procedureExecutingBatchSize", r.procedureExecutingBatchSize))
			break
		}

		// Mark the shard assigned.
		assignedShardIDs[shardNode.ID] = struct{}{}
		newLeaderNode, ok := shardNodeMapping[shardNode.ID]
		assert.Assert(ok)
		if newLeaderNode.Node.Name != shardNode.NodeName {
			r.logger.Info("rebalanced shard scheduler try to assign shard to another node", zap.Uint64("shardID", uint64(shardNode.ID)), zap.String("originNode", shardNode.NodeName), zap.String("newNode", newLeaderNode.Node.Name))
			p, err := r.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
				Snapshot:          clusterSnapshot,
				ShardID:           shardNode.ID,
				OldLeaderNodeName: shardNode.NodeName,
				NewLeaderNodeName: newLeaderNode.Node.Name,
			})
			if err != nil {
				return emptySchedulerRes, err
			}

			procedures = append(procedures, p)
			reasons.WriteString(fmt.Sprintf("shard is transferred to another node, shardID:%d, oldNode:%s, newNode:%s\n", shardNode.ID, shardNode.NodeName, newLeaderNode.Node.Name))
		}
	}

	// Check whether the assigned shard needs to be reopened.
	for id := uint32(0); id < numShards; id++ {
		if len(procedures) >= int(r.procedureExecutingBatchSize) {
			r.logger.Warn("procedure length reached procedure executing batch size", zap.Uint32("procedureExecutingBatchSize", r.procedureExecutingBatchSize))
			break
		}

		shardID := storage.ShardID(id)
		if _, assigned := assignedShardIDs[shardID]; !assigned {
			node, ok := r.latestShardNodeMapping[shardID]
			assert.Assert(ok)

			r.logger.Info("rebalanced shard scheduler try to assign unassigned shard to node", zap.Uint32("shardID", id), zap.String("node", node.Node.Name))
			p, err := r.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
				Snapshot:          clusterSnapshot,
				ShardID:           shardID,
				OldLeaderNodeName: "",
				NewLeaderNodeName: node.Node.Name,
			})
			if err != nil {
				return emptySchedulerRes, err
			}

			procedures = append(procedures, p)
			reasons.WriteString(fmt.Sprintf("shard is assigned to a node, shardID:%d, node:%s\n", shardID, node.Node.Name))
		}
	}

	if len(procedures) == 0 {
		return emptySchedulerRes, nil
	}

	batchProcedure, err := r.factory.CreateBatchTransferLeaderProcedure(ctx, coordinator.BatchRequest{
		Batch:     procedures,
		BatchType: procedure.TransferLeader,
	})
	if err != nil {
		return emptySchedulerRes, err
	}

	return scheduler.ScheduleResult{Procedure: batchProcedure, Reason: reasons.String()}, nil
}

func (r *schedulerImpl) generateLatestShardNodeMapping(ctx context.Context, snapshot metadata.Snapshot) (map[storage.ShardID]metadata.RegisteredNode, error) {
	numShards := uint32(len(snapshot.Topology.ShardViewsMapping))
	// TODO: Improve scheduling efficiency and verify whether the topology changes.
	shardIDs := make([]storage.ShardID, 0, numShards)
	for shardID := range snapshot.Topology.ShardViewsMapping {
		shardIDs = append(shardIDs, shardID)
	}

	r.lock.Lock()
	defer r.lock.Unlock()
	var err error
	shardNodeMapping := r.latestShardNodeMapping
	if !r.deployMode {
		pickConfig := nodepicker.Config{
			NumTotalShards:    numShards,
			ShardAffinityRule: maps.Clone(r.shardAffinityRule),
		}
		shardNodeMapping, err = r.nodePicker.PickNode(ctx, pickConfig, shardIDs, snapshot.RegisteredNodes)
		if err != nil {
			return nil, err
		}
		r.latestShardNodeMapping = shardNodeMapping
	}

	return shardNodeMapping, nil
}

func (r *schedulerImpl) updateDeployMode(deployMode bool) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.deployMode = deployMode
}
