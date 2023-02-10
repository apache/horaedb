// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"sync"
	"time"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	heartbeatCheckInterval               = 10 * time.Second
	heartbeatKeepAliveIntervalSec uint64 = 15
)

type Scheduler struct {
	// This lock is used to protect the field `running`.
	lock    sync.RWMutex
	running bool

	clusterManager   cluster.Manager
	procedureManager procedure.Manager
	procedureFactory *Factory
	dispatch         eventdispatch.Dispatch

	checkNodeTicker *time.Ticker
}

func NewScheduler(clusterManager cluster.Manager, procedureManager procedure.Manager, procedureFactory *Factory, dispatch eventdispatch.Dispatch) *Scheduler {
	return &Scheduler{
		running:          false,
		clusterManager:   clusterManager,
		procedureManager: procedureManager,
		procedureFactory: procedureFactory,
		dispatch:         dispatch,
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.running {
		log.Warn("scheduler has already been started")
		return nil
	}

	s.running = true
	ticker := time.NewTicker(heartbeatCheckInterval)
	s.checkNodeTicker = ticker
	go s.checkNode(ctx, ticker)
	return nil
}

func (s *Scheduler) Stop(_ context.Context) error {
	s.checkNodeTicker.Stop()
	return nil
}

func (s *Scheduler) ProcessHeartbeat(_ context.Context, _ *metaservicepb.NodeInfo) {
	// Check node version and update to latest.
}

func (s *Scheduler) checkNode(ctx context.Context, ticker *time.Ticker) {
	for t := range ticker.C {
		clusters, err := s.clusterManager.ListClusters(ctx)
		if err != nil {
			log.Error("list clusters failed", zap.Error(err))
			continue
		}
		for _, c := range clusters {
			nodes := c.GetRegisteredNodes()
			nodeShards, err := c.GetNodeShards(ctx)
			if err != nil {
				log.Error("get node shards failed", zap.Error(err))
				continue
			}
			nodeShardsMapping := map[string][]cluster.ShardInfo{}
			for _, nodeShard := range nodeShards.NodeShards {
				_, exists := nodeShardsMapping[nodeShard.ShardNode.NodeName]
				if !exists {
					nodeShardsMapping[nodeShard.ShardNode.NodeName] = []cluster.ShardInfo{}
				}
				nodeShardsMapping[nodeShard.ShardNode.NodeName] = append(nodeShardsMapping[nodeShard.ShardNode.NodeName], cluster.ShardInfo{
					ID:      nodeShard.ShardNode.ID,
					Role:    nodeShard.ShardNode.ShardRole,
					Version: nodeShard.ShardInfo.Version,
				})
			}
			s.processNodes(ctx, nodes, t, nodeShardsMapping)
		}
	}
}

func (s *Scheduler) processNodes(ctx context.Context, nodes []cluster.RegisteredNode, t time.Time, nodeShardsMapping map[string][]cluster.ShardInfo) {
	for _, node := range nodes {
		// Determines whether node is expired.
		if !node.IsExpired(uint64(t.Unix()), heartbeatKeepAliveIntervalSec) {
			// Shard versions of CeresDB and CeresMeta may be inconsistent. And close extra shards and open missing shards if so.
			realShards := node.ShardInfos
			expectShards := nodeShardsMapping[node.Node.Name]
			err := s.applyMetadataShardInfo(ctx, node.Node.Name, realShards, expectShards)
			if err != nil {
				log.Error("apply metadata failed", zap.Error(err))
			}
		}
	}
}

// applyMetadataShardInfo verify shardInfo in heartbeats and metadata, they are forcibly synchronized to the latest version if they are inconsistent.
// TODO: Encapsulate the following logic as a standalone ApplyProcedure.
func (s *Scheduler) applyMetadataShardInfo(ctx context.Context, node string, realShards []cluster.ShardInfo, expectShards []cluster.ShardInfo) error {
	realShardInfoMapping := make(map[storage.ShardID]cluster.ShardInfo, len(realShards))
	expectShardInfoMapping := make(map[storage.ShardID]cluster.ShardInfo, len(expectShards))
	for _, realShard := range realShards {
		realShardInfoMapping[realShard.ID] = realShard
	}
	for _, expectShard := range expectShards {
		expectShardInfoMapping[expectShard.ID] = expectShard
	}

	// This includes the following cases:
	for _, expectShard := range expectShards {
		realShard, exists := realShardInfoMapping[expectShard.ID]

		// 1. Shard exists in metadata and not exists in node, reopen lack shards on node.
		if !exists {
			log.Info("Shard exists in metadata and not exists in node, reopen lack shards on node.", zap.String("node", node), zap.Uint32("shardID", uint32(expectShard.ID)))
			if err := s.dispatch.OpenShard(ctx, node, eventdispatch.OpenShardRequest{Shard: expectShard}); err != nil {
				return errors.WithMessagef(err, "reopen shard failed, shardInfo:%d", expectShard.ID)
			}
			continue
		}

		// 2. Shard exists in both metadata and node, versions are consistent, do nothing.
		if realShard.Version == expectShard.Version {
			continue
		}

		// 3. Shard exists in both metadata and node, versions are inconsistent, close and reopen invalid shard on node.
		log.Info("Shard exists in both metadata and node, versions are inconsistent, close and reopen invalid shard on node.", zap.String("node", node), zap.Uint32("shardID", uint32(expectShard.ID)))
		if err := s.dispatch.CloseShard(ctx, node, eventdispatch.CloseShardRequest{ShardID: uint32(expectShard.ID)}); err != nil {
			return errors.WithMessagef(err, "close shard failed, shardInfo:%d", expectShard.ID)
		}
		if err := s.dispatch.OpenShard(ctx, node, eventdispatch.OpenShardRequest{Shard: expectShard}); err != nil {
			return errors.WithMessagef(err, "reopen shard failed, shardInfo:%d", expectShard.ID)
		}
	}

	// 4. Shard exists in node and not exists in metadata, close extra shard on node.
	for _, realShard := range realShards {
		_, ok := expectShardInfoMapping[realShard.ID]
		if ok {
			continue
		}
		log.Info("Shard exists in node and not exists in metadata, close extra shard on node.", zap.String("node", node), zap.Uint32("shardID", uint32(realShard.ID)))
		if err := s.dispatch.CloseShard(ctx, node, eventdispatch.CloseShardRequest{ShardID: uint32(realShard.ID)}); err != nil {
			return errors.WithMessagef(err, "close shard failed, shardInfo:%d", realShard.ID)
		}
	}

	return nil
}
