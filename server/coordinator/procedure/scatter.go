// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sync"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	eventScatterPrepare = "EventScatterPrepare"
	eventScatterFailed  = "EventScatterFailed"
	eventScatterSuccess = "EventScatterSuccess"

	stateScatterBegin   = "StateScatterBegin"
	stateScatterWaiting = "StateScatterWaiting"
	stateScatterFinish  = "StateScatterFinish"
	stateScatterFailed  = "StateScatterFailed"

	defaultCheckNodeNumTimeInterval = time.Second * 3
)

var (
	scatterEvents = fsm.Events{
		{Name: eventScatterPrepare, Src: []string{stateScatterBegin}, Dst: stateScatterWaiting},
		{Name: eventScatterSuccess, Src: []string{stateScatterWaiting}, Dst: stateScatterFinish},
		{Name: eventScatterFailed, Src: []string{stateScatterWaiting}, Dst: stateScatterFailed},
	}
	scatterCallbacks = fsm.Callbacks{
		eventScatterPrepare: scatterPrepareCallback,
		eventScatterFailed:  scatterFailedCallback,
		eventScatterSuccess: scatterSuccessCallback,
	}
)

func scatterPrepareCallback(event *fsm.Event) {
	request := event.Args[0].(*ScatterCallbackRequest)
	c := request.cluster
	ctx := request.ctx

	waitForNodesReady(c)

	nodeCache := c.GetClusterNodeCache()
	shardTotal := c.GetClusterShardTotal()
	minNodeCount := c.GetClusterMinNodeCount()

	if !(c.GetClusterState() == clusterpb.ClusterTopology_EMPTY) {
		event.Cancel(errors.WithMessage(cluster.ErrClusterStateInvalid, "cluster topology state is not empty"))
		return
	}

	nodeList := make([]*clusterpb.Node, 0, len(nodeCache))
	for _, v := range nodeCache {
		nodeList = append(nodeList, v.GetMeta())
	}

	shards, err := allocNodeShards(ctx, shardTotal, minNodeCount, nodeList, request.allocator)
	if err != nil {
		event.Cancel(errors.WithMessage(err, "alloc node shards failed"))
		return
	}

	for nodeName, node := range nodeCache {
		for _, shardID := range node.GetShardIDs() {
			openShardRequest := &eventdispatch.OpenShardRequest{
				Shard: &cluster.ShardInfo{
					ShardID:   shardID,
					ShardRole: clusterpb.ShardRole_LEADER,
				},
			}

			if err := request.dispatch.OpenShard(ctx, nodeName, openShardRequest); err != nil {
				event.Cancel(errors.WithMessage(err, "open shard failed"))
				return
			}
		}
	}

	if err := c.UpdateClusterTopology(ctx, clusterpb.ClusterTopology_STABLE, shards); err != nil {
		event.Cancel(errors.WithMessage(err, "update cluster topology failed"))
		return
	}
}

func waitForNodesReady(c *cluster.Cluster) {
	for {
		time.Sleep(defaultCheckNodeNumTimeInterval)
		currNodeNum := uint32(c.GetNodesSize())
		expectNodeNum := c.GetClusterMinNodeCount()
		if currNodeNum < expectNodeNum {
			log.Warn("wait for cluster node register", zap.Uint32("currNodeNum", currNodeNum), zap.Uint32("expectNodeNum", expectNodeNum))
			continue
		}
		break
	}
}

func allocNodeShards(cxt context.Context, shardTotal uint32, minNodeCount uint32, nodeList []*clusterpb.Node, allocator id.Allocator) ([]*clusterpb.Shard, error) {
	shards := make([]*clusterpb.Shard, 0, shardTotal)

	perNodeShardCount := shardTotal / minNodeCount
	if shardTotal%minNodeCount != 0 {
		perNodeShardCount++
	}

	for i := uint32(0); i < minNodeCount; i++ {
		for j := uint32(0); j < perNodeShardCount; j++ {
			ID, err := allocator.Alloc(cxt)
			shardID := uint32(ID)
			if err != nil {
				return nil, errors.WithMessage(err, "alloc shard id failed")
			}
			if shardID < shardTotal {
				// TODO: consider nodesCache state
				shards = append(shards, &clusterpb.Shard{
					Id:        shardID,
					ShardRole: clusterpb.ShardRole_LEADER,
					Node:      nodeList[i].GetName(),
				})
			}
		}
	}

	return shards, nil
}

func scatterSuccessCallback(event *fsm.Event) {
	request := event.Args[0].(*ScatterCallbackRequest)

	if err := request.cluster.Load(request.ctx); err != nil {
		event.Cancel(errors.WithMessage(err, "coordinator scatterShard"))
		return
	}
}

func scatterFailedCallback(_ *fsm.Event) {
	// TODO: Use RollbackProcedure to rollback transfer failed
}

// ScatterCallbackRequest is fsm callbacks param.
type ScatterCallbackRequest struct {
	cluster   *cluster.Cluster
	ctx       context.Context
	dispatch  eventdispatch.Dispatch
	allocator id.Allocator
}

func NewScatterProcedure(dispatch eventdispatch.Dispatch, cluster *cluster.Cluster, id uint64, shardIDAllocator id.Allocator) Procedure {
	scatterProcedureFsm := fsm.NewFSM(
		stateScatterBegin,
		scatterEvents,
		scatterCallbacks,
	)
	return &ScatterProcedure{id: id, state: StateInit, fsm: scatterProcedureFsm, dispatch: dispatch, cluster: cluster, allocator: shardIDAllocator}
}

type ScatterProcedure struct {
	lock     sync.RWMutex
	id       uint64
	state    State
	fsm      *fsm.FSM
	dispatch eventdispatch.Dispatch

	cluster   *cluster.Cluster
	allocator id.Allocator
}

func (p *ScatterProcedure) ID() uint64 {
	return p.id
}

func (p *ScatterProcedure) Typ() Typ {
	return Scatter
}

func (p *ScatterProcedure) Start(ctx context.Context) error {
	p.UpdateStateWithLock(StateRunning)

	scatterCallbackRequest := &ScatterCallbackRequest{
		cluster:   p.cluster,
		ctx:       ctx,
		dispatch:  p.dispatch,
		allocator: p.allocator,
	}

	if err := p.fsm.Event(eventScatterPrepare, scatterCallbackRequest); err != nil {
		err := p.fsm.Event(eventScatterFailed, scatterCallbackRequest)
		p.UpdateStateWithLock(StateFailed)
		return errors.WithMessage(err, "coordinator transferLeaderShard start")
	}

	if err := p.fsm.Event(eventScatterSuccess, scatterCallbackRequest); err != nil {
		return errors.WithMessage(err, "coordinator transferLeaderShard start")
	}

	p.UpdateStateWithLock(StateFinished)
	return nil
}

func (p *ScatterProcedure) Cancel(_ context.Context) error {
	p.UpdateStateWithLock(StateCancelled)
	return nil
}

func (p *ScatterProcedure) State() State {
	return p.state
}

func (p *ScatterProcedure) UpdateStateWithLock(state State) {
	p.lock.Lock()
	p.state = state
	p.lock.Unlock()
}
