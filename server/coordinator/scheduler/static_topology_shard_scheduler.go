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
	"github.com/pkg/errors"
)

type StaticTopologyShardScheduler struct {
	factory                     *coordinator.Factory
	nodePicker                  coordinator.NodePicker
	procedureExecutingBatchSize uint32
}

func NewStaticTopologyShardScheduler(factory *coordinator.Factory, nodePicker coordinator.NodePicker, procedureExecutingBatchSize uint32) Scheduler {
	return &StaticTopologyShardScheduler{factory: factory, nodePicker: nodePicker, procedureExecutingBatchSize: procedureExecutingBatchSize}
}

func (s *StaticTopologyShardScheduler) Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (ScheduleResult, error) {
	var procedures []procedure.Procedure
	var reasons strings.Builder

	switch clusterSnapshot.Topology.ClusterView.State {
	case storage.ClusterStateEmpty:
		return ScheduleResult{}, nil
	case storage.ClusterStatePrepare:
		unassignedShardIds := make([]storage.ShardID, 0, len(clusterSnapshot.Topology.ShardViewsMapping))
		for _, shardView := range clusterSnapshot.Topology.ShardViewsMapping {
			_, exists := findNodeByShard(shardView.ShardID, clusterSnapshot.Topology.ClusterView.ShardNodes)
			if exists {
				continue
			}
			unassignedShardIds = append(unassignedShardIds, shardView.ShardID)
		}
		// Assign shards
		shardNodeMapping, err := s.nodePicker.PickNode(ctx, unassignedShardIds, uint32(len(clusterSnapshot.Topology.ShardViewsMapping)), clusterSnapshot.RegisteredNodes)
		if err != nil {
			return ScheduleResult{}, err
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
				return ScheduleResult{}, err
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
				return ScheduleResult{}, err
			}
			if !contains(shardNode.ID, node.ShardInfos) {
				// Shard need to be reopened
				p, err := s.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
					Snapshot:          clusterSnapshot,
					ShardID:           shardNode.ID,
					OldLeaderNodeName: "",
					NewLeaderNodeName: node.Node.Name,
				})
				if err != nil {
					return ScheduleResult{}, err
				}
				procedures = append(procedures, p)
				reasons.WriteString(fmt.Sprintf("Cluster initialization, assign shard to node, shardID:%d, nodeName:%s. ", shardNode.ID, node.Node.Name))
				if len(procedures) >= int(s.procedureExecutingBatchSize) {
					break
				}
			}
		}
	}

	if len(procedures) == 0 {
		return ScheduleResult{}, nil
	}

	batchProcedure, err := s.factory.CreateBatchTransferLeaderProcedure(ctx, coordinator.BatchRequest{
		Batch:     procedures,
		BatchType: procedure.TransferLeader,
	})
	if err != nil {
		return ScheduleResult{}, err
	}

	return ScheduleResult{batchProcedure, reasons.String()}, nil
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

func contains(shardID storage.ShardID, shardInfos []metadata.ShardInfo) bool {
	for i := 0; i < len(shardInfos); i++ {
		if shardInfos[i].ID == shardID {
			return true
		}
	}
	return false
}
