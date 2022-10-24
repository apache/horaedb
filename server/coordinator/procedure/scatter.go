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

	// When CeresMeta leader node restarts after the cluster has been initialized, the clusterTopology state is STABLE which is not an illegal state, that is to say, there is no need to do scatter work.
	// Just print some warning log and return, do not cancel event
	if !(c.GetClusterState() == clusterpb.ClusterTopology_EMPTY) {
		log.Warn("cluster topology state is not empty")
		return
	}

	nodeList := make([]*clusterpb.Node, 0, len(nodeCache))
	for _, v := range nodeCache {
		nodeList = append(nodeList, v.GetMeta())
	}

	shards, err := allocNodeShards(ctx, shardTotal, minNodeCount, nodeList, request.shardIDs)
	if err != nil {
		cancelEventWithLog(event, err, "alloc node shards failed")
		return
	}

	shardTopologies := make([]*clusterpb.ShardTopology, 0, len(shards))
	for _, shard := range shards {
		shardTopologies = append(shardTopologies, &clusterpb.ShardTopology{
			ShardId:  shard.GetId(),
			TableIds: []uint64{},
			Version:  0,
		})
	}

	if err := c.CreateShardTopologies(ctx, shardTopologies); err != nil {
		cancelEventWithLog(event, err, "create shard topologies failed")
		return
	}

	if err := c.UpdateClusterTopology(ctx, clusterpb.ClusterTopology_STABLE, shards); err != nil {
		cancelEventWithLog(event, err, "update cluster topology failed")
		return
	}

	if err := request.cluster.Load(request.ctx); err != nil {
		cancelEventWithLog(event, err, "cluster load data failed")
		return
	}

	for _, shard := range shards {
		openShardRequest := &eventdispatch.OpenShardRequest{
			Shard: &cluster.ShardInfo{
				ID:   shard.GetId(),
				Role: clusterpb.ShardRole_LEADER,
			},
		}
		if err := request.dispatch.OpenShard(ctx, shard.Node, openShardRequest); err != nil {
			cancelEventWithLog(event, err, "open shard failed")
			return
		}
	}
}

func waitForNodesReady(c *cluster.Cluster) {
	for {
		time.Sleep(defaultCheckNodeNumTimeInterval)
		nodes := c.GetNodes()
		currNodeNum := getAvailableNodesNum(nodes)
		expectNodeNum := c.GetClusterMinNodeCount()
		log.Warn("wait for cluster node register", zap.Uint32("currNodeNum", currNodeNum), zap.Uint32("expectNodeNum", expectNodeNum))
		if currNodeNum < expectNodeNum {
			continue
		}
		break
	}
}

func getAvailableNodesNum(nodes []*cluster.Node) uint32 {
	nodeSize := uint32(0)
	for _, node := range nodes {
		if node.IsAvailable() {
			nodeSize++
		}
	}
	return nodeSize
}

func allocNodeShards(_ context.Context, shardTotal uint32, minNodeCount uint32, nodeList []*clusterpb.Node, shardIDs []uint32) ([]*clusterpb.Shard, error) {
	shards := make([]*clusterpb.Shard, 0, shardTotal)

	perNodeShardCount := shardTotal / minNodeCount
	if shardTotal%minNodeCount != 0 {
		perNodeShardCount++
	}

	for i := uint32(0); i < minNodeCount; i++ {
		for j := uint32(0); j < perNodeShardCount; j++ {
			if uint32(len(shards)) < shardTotal {
				shardID := shardIDs[len(shards)]
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
		cancelEventWithLog(event, err, "cluster load data failed")
		return
	}
	log.Info("scatter procedure execute finish")
}

func scatterFailedCallback(_ *fsm.Event) {
	// TODO: Use RollbackProcedure to rollback transfer failed
}

// ScatterCallbackRequest is fsm callbacks param.
type ScatterCallbackRequest struct {
	cluster  *cluster.Cluster
	ctx      context.Context
	dispatch eventdispatch.Dispatch
	shardIDs []uint32
}

func NewScatterProcedure(dispatch eventdispatch.Dispatch, cluster *cluster.Cluster, id uint64, shardIDs []uint32) Procedure {
	scatterProcedureFsm := fsm.NewFSM(
		stateScatterBegin,
		scatterEvents,
		scatterCallbacks,
	)

	return &ScatterProcedure{id: id, state: StateInit, fsm: scatterProcedureFsm, dispatch: dispatch, cluster: cluster, shardIDs: shardIDs}
}

type ScatterProcedure struct {
	id       uint64
	fsm      *fsm.FSM
	dispatch eventdispatch.Dispatch
	cluster  *cluster.Cluster
	shardIDs []uint32

	lock  sync.RWMutex
	state State
}

func (p *ScatterProcedure) ID() uint64 {
	return p.id
}

func (p *ScatterProcedure) Typ() Typ {
	return Scatter
}

func (p *ScatterProcedure) Start(ctx context.Context) error {
	p.updateStateWithLock(StateRunning)

	scatterCallbackRequest := &ScatterCallbackRequest{
		cluster:  p.cluster,
		ctx:      ctx,
		dispatch: p.dispatch,
		shardIDs: p.shardIDs,
	}

	if err := p.fsm.Event(eventScatterPrepare, scatterCallbackRequest); err != nil {
		err1 := p.fsm.Event(eventScatterFailed, scatterCallbackRequest)
		p.updateStateWithLock(StateFailed)
		if err1 != nil {
			err = errors.WithMessagef(err, "scatter procedure start, fail to send eventScatterFailed err:%v", err1)
		}
		return errors.WithMessage(err, "scatter procedure start")
	}

	if err := p.fsm.Event(eventScatterSuccess, scatterCallbackRequest); err != nil {
		return errors.WithMessage(err, "scatter procedure start")
	}

	p.updateStateWithLock(StateFinished)
	return nil
}

func (p *ScatterProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(StateCancelled)
	return nil
}

func (p *ScatterProcedure) State() State {
	return p.state
}

func (p *ScatterProcedure) updateStateWithLock(state State) {
	p.lock.Lock()
	p.state = state
	p.lock.Unlock()
}
