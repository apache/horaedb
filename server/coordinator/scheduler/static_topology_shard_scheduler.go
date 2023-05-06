// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

type StaticTopologyShardScheduler struct {
	factory    *coordinator.Factory
	nodePicker coordinator.NodePicker
}

func NewStaticTopologyShardScheduler(factory *coordinator.Factory, nodePicker coordinator.NodePicker) Scheduler {
	return &StaticTopologyShardScheduler{factory: factory, nodePicker: nodePicker}
}

func (s *StaticTopologyShardScheduler) Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (ScheduleResult, error) {
	switch clusterSnapshot.Topology.ClusterView.State {
	case storage.ClusterStateEmpty:
		return ScheduleResult{}, nil
	case storage.ClusterStatePrepare:
		for _, shardView := range clusterSnapshot.Topology.ShardViewsMapping {
			_, exists := findNodeByShard(shardView.ShardID, clusterSnapshot.Topology.ClusterView.ShardNodes)
			if exists {
				continue
			}
			// Assign shards
			newLeaderNode, err := s.nodePicker.PickNode(ctx, shardView.ShardID, clusterSnapshot.RegisteredNodes)
			if err != nil {
				return ScheduleResult{}, err
			}
			// Shard exists and ShardNode not exists.
			p, err := s.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
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
				Reason:    fmt.Sprintf("Cluster initialization, shard:%d is assigned to node:%s", shardView.ShardID, newLeaderNode.Node.Name),
			}, nil
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
				return ScheduleResult{
					Procedure: p,
					Reason:    fmt.Sprintf("Cluster state is stable, shard:%d is reopened in node:%s", shardNode.ID, node.Node.Name),
				}, nil
			}
		}
	}

	return ScheduleResult{}, nil
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
