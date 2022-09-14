// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/schedule"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// TODO: heartbeatInterval should be set in config
const DefaultHeartbeatInterval = time.Second * 3

// coordinator is used to decide if the shards need to be scheduled.
type coordinator struct {
	// Mutex is to ensure only one coordinator can run at the same time.
	lock    sync.Mutex
	cluster *Cluster

	ctx          context.Context
	cancel       context.CancelFunc
	bgJobWg      *sync.WaitGroup
	eventHandler *schedule.EventHandler
}

func newCoordinator(cluster *Cluster, hbstreams *schedule.HeartbeatStreams) *coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &coordinator{
		cluster: cluster,

		ctx:          ctx,
		cancel:       cancel,
		bgJobWg:      &sync.WaitGroup{},
		eventHandler: schedule.NewEventHandler(hbstreams),
	}
}

func (c *coordinator) stop() {
	c.cancel()
	c.bgJobWg.Wait()
}

func (c *coordinator) runBgJob() {
	c.bgJobWg.Add(1)
	defer c.bgJobWg.Done()

	log.Info("coordinator runBgJob started", zap.String("cluster", c.cluster.Name()))
	for {
		t := time.After(DefaultHeartbeatInterval)
		select {
		case <-t:
			for nodeName := range c.cluster.nodesCache {
				err := c.eventHandler.Dispatch(c.ctx, nodeName, &schedule.NoneEvent{})
				if err != nil {
					log.Error("fail to send node event msg", zap.Error(err), zap.String("node", nodeName))
				}
			}

		case <-c.ctx.Done():
			log.Warn("exit from background jobs")
			return
		}
	}
}

func (c *coordinator) Close() {
	c.cancel()
	c.bgJobWg.Wait()
}

// TODO: consider ReplicationFactor
func (c *coordinator) scatterShard(ctx context.Context, nodeInfo *metaservicepb.NodeInfo) error {
	// FIXME: The following logic is used for static topology, which is a bit simple and violent.
	// It needs to be modified when supporting dynamic topology.
	if c.cluster.metaData.clusterTopology.State == clusterpb.ClusterTopology_STABLE {
		shardIDs, err := c.cluster.GetShardIDs(nodeInfo.GetEndpoint())
		if err != nil {
			return errors.WithMessage(err, "coordinator scatterShard")
		}
		if len(nodeInfo.GetShardInfos()) == 0 {
			if err := c.eventHandler.Dispatch(ctx, nodeInfo.GetEndpoint(), &schedule.OpenEvent{ShardIDs: shardIDs}); err != nil {
				return errors.WithMessage(err, "coordinator scatterShard")
			}
		}
	}

	if !(int(c.cluster.metaData.cluster.MinNodeCount) <= len(c.cluster.nodesCache) &&
		c.cluster.metaData.clusterTopology.State == clusterpb.ClusterTopology_EMPTY) {
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	shardTotal := int(c.cluster.metaData.cluster.ShardTotal)
	minNodeCount := int(c.cluster.metaData.cluster.MinNodeCount)
	perNodeShardCount := shardTotal / minNodeCount
	shards := make([]*clusterpb.Shard, 0, shardTotal)
	nodeList := make([]*clusterpb.Node, 0, len(c.cluster.nodesCache))
	for _, v := range c.cluster.nodesCache {
		nodeList = append(nodeList, v.meta)
	}

	for i := 0; i < minNodeCount; i++ {
		for j := 0; j < perNodeShardCount; j++ {
			if i*perNodeShardCount+j < shardTotal {
				// TODO: consider nodesCache state
				shards = append(shards, &clusterpb.Shard{
					Id:        uint32(i*perNodeShardCount + j),
					ShardRole: clusterpb.ShardRole_LEADER,
					Node:      nodeList[i].GetName(),
				})
			}
		}
	}

	c.cluster.metaData.clusterTopology.ShardView = shards
	c.cluster.metaData.clusterTopology.State = clusterpb.ClusterTopology_STABLE
	if err := c.cluster.storage.PutClusterTopology(ctx, c.cluster.clusterID, c.cluster.metaData.clusterTopology.Version, c.cluster.metaData.clusterTopology); err != nil {
		return errors.WithMessage(err, "coordinator scatterShard")
	}

	if err := c.cluster.Load(ctx); err != nil {
		return errors.WithMessage(err, "coordinator scatterShard")
	}

	for nodeName, node := range c.cluster.nodesCache {
		if err := c.eventHandler.Dispatch(ctx, nodeName, &schedule.OpenEvent{ShardIDs: node.shardIDs}); err != nil {
			return errors.WithMessage(err, "coordinator scatterShard")
		}
	}
	return nil
}
