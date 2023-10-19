// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package static

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/scheduler"
	"github.com/CeresDB/ceresmeta/server/coordinator/scheduler/nodepicker"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

type schedulerImpl struct {
	factory                     *coordinator.Factory
	nodePicker                  nodepicker.NodePicker
	procedureExecutingBatchSize uint32
}

func NewShardScheduler(factory *coordinator.Factory, nodePicker nodepicker.NodePicker, procedureExecutingBatchSize uint32) scheduler.Scheduler {
	return schedulerImpl{factory: factory, nodePicker: nodePicker, procedureExecutingBatchSize: procedureExecutingBatchSize}
}

func (s schedulerImpl) Name() string {
	return "static_scheduler"
}

func (s schedulerImpl) UpdateDeployMode(_ context.Context, _ bool) {
	// StaticTopologyShardScheduler do not need deployMode.
}

func (s schedulerImpl) AddShardAffinityRule(_ context.Context, _ scheduler.ShardAffinityRule) error {
	return ErrNotImplemented.WithCausef("static topology scheduler doesn't support shard affinity")
}

func (s schedulerImpl) RemoveShardAffinityRule(_ context.Context, _ storage.ShardID) error {
	return ErrNotImplemented.WithCausef("static topology scheduler doesn't support shard affinity")
}

func (s schedulerImpl) ListShardAffinityRule(_ context.Context) (scheduler.ShardAffinityRule, error) {
	var emptyRule scheduler.ShardAffinityRule
	return emptyRule, ErrNotImplemented.WithCausef("static topology scheduler doesn't support shard affinity")
}

func (s schedulerImpl) Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (scheduler.ScheduleResult, error) {
	var procedures []procedure.Procedure
	var reasons strings.Builder
	var emptyScheduleRes scheduler.ScheduleResult

	switch clusterSnapshot.Topology.ClusterView.State {
	case storage.ClusterStateEmpty:
		return emptyScheduleRes, nil
	case storage.ClusterStatePrepare:
		unassignedShardIds := make([]storage.ShardID, 0, len(clusterSnapshot.Topology.ShardViewsMapping))
		for _, shardView := range clusterSnapshot.Topology.ShardViewsMapping {
			_, exists := findNodeByShard(shardView.ShardID, clusterSnapshot.Topology.ClusterView.ShardNodes)
			if exists {
				continue
			}
			unassignedShardIds = append(unassignedShardIds, shardView.ShardID)
		}
		pickConfig := nodepicker.Config{
			NumTotalShards:    uint32(len(clusterSnapshot.Topology.ShardViewsMapping)),
			ShardAffinityRule: map[storage.ShardID]scheduler.ShardAffinity{},
		}
		// Assign shards
		shardNodeMapping, err := s.nodePicker.PickNode(ctx, pickConfig, unassignedShardIds, clusterSnapshot.RegisteredNodes)
		if err != nil {
			return emptyScheduleRes, err
		}
		for shardID, node := range shardNodeMapping {
			// Shard exists and ShardNode not exists.
			p, err := s.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
				Snapshot:          clusterSnapshot,
				ShardID:           shardID,
				OldLeaderNodeName: "",
				NewLeaderNodeName: node.Node.Name,
			})
			if err != nil {
				return emptyScheduleRes, err
			}
			procedures = append(procedures, p)
			reasons.WriteString(fmt.Sprintf("Cluster initialization, assign shard to node, shardID:%d, nodeName:%s. ", shardID, node.Node.Name))
			if len(procedures) >= int(s.procedureExecutingBatchSize) {
				break
			}
		}
	case storage.ClusterStateStable:
		for i := 0; i < len(clusterSnapshot.Topology.ClusterView.ShardNodes); i++ {
			shardNode := clusterSnapshot.Topology.ClusterView.ShardNodes[i]
			node, err := findOnlineNodeByName(shardNode.NodeName, clusterSnapshot.RegisteredNodes)
			if err != nil {
				continue
			}
			if !containsShard(node.ShardInfos, shardNode.ID) {
				// Shard need to be reopened
				p, err := s.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
					Snapshot:          clusterSnapshot,
					ShardID:           shardNode.ID,
					OldLeaderNodeName: "",
					NewLeaderNodeName: node.Node.Name,
				})
				if err != nil {
					return emptyScheduleRes, err
				}
				procedures = append(procedures, p)
				reasons.WriteString(fmt.Sprintf("Cluster recover, assign shard to node, shardID:%d, nodeName:%s. ", shardNode.ID, node.Node.Name))
				if len(procedures) >= int(s.procedureExecutingBatchSize) {
					break
				}
			}
		}
	}

	if len(procedures) == 0 {
		return emptyScheduleRes, nil
	}

	batchProcedure, err := s.factory.CreateBatchTransferLeaderProcedure(ctx, coordinator.BatchRequest{
		Batch:     procedures,
		BatchType: procedure.TransferLeader,
	})
	if err != nil {
		return emptyScheduleRes, err
	}

	return scheduler.ScheduleResult{Procedure: batchProcedure, Reason: reasons.String()}, nil
}

func findOnlineNodeByName(nodeName string, nodes []metadata.RegisteredNode) (metadata.RegisteredNode, error) {
	now := time.Now()
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		if node.IsExpired(now) {
			continue
		}
		if node.Node.Name == nodeName {
			return node, nil
		}
	}

	return metadata.RegisteredNode{}, errors.WithMessagef(metadata.ErrNodeNotFound, "node:%s not found in topology", nodeName)
}

func containsShard(shardInfos []metadata.ShardInfo, shardID storage.ShardID) bool {
	for i := 0; i < len(shardInfos); i++ {
		if shardInfos[i].ID == shardID {
			return true
		}
	}
	return false
}

func findNodeByShard(shardID storage.ShardID, shardNodes []storage.ShardNode) (storage.ShardNode, bool) {
	n, found := slices.BinarySearchFunc(shardNodes, shardID, func(node storage.ShardNode, id storage.ShardID) int {
		return cmp.Compare(node.ID, id)
	})
	if !found {
		var emptyShardNode storage.ShardNode
		return emptyShardNode, false
	}
	return shardNodes[n], true
}
