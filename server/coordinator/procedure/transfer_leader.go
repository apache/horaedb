// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"fmt"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	eventTransferLeaderPrepare = "EventTransferLeaderPrepare"
	eventTransferLeaderFailed  = "EventTransferLeaderFailed"
	eventTransferLeaderSuccess = "EventTransferLeaderSuccess"

	stateTransferLeaderBegin   = "StateTransferLeaderBegin"
	stateTransferLeaderWaiting = "StateTransferLeaderWaiting"
	stateTransferLeaderFinish  = "StateTransferLeaderFinish"
	stateTransferLeaderFailed  = "StateTransferLeaderFailed"
)

var (
	transferLeaderEvents = fsm.Events{
		{Name: eventTransferLeaderPrepare, Src: []string{stateTransferLeaderBegin}, Dst: stateTransferLeaderWaiting},
		{Name: eventTransferLeaderSuccess, Src: []string{stateTransferLeaderWaiting}, Dst: stateTransferLeaderFinish},
		{Name: eventTransferLeaderFailed, Src: []string{stateTransferLeaderWaiting}, Dst: stateTransferLeaderFailed},
	}
	transferLeaderCallbacks = fsm.Callbacks{
		eventTransferLeaderPrepare: transferLeaderPrepareCallback,
		eventTransferLeaderFailed:  transferLeaderFailedCallback,
		eventTransferLeaderSuccess: transferLeaderSuccessCallback,
	}
)

type TransferLeaderProcedure struct {
	id        uint64
	fsm       *fsm.FSM
	dispatch  eventdispatch.Dispatch
	cluster   *cluster.Cluster
	oldLeader *clusterpb.Shard
	newLeader *clusterpb.Shard

	// Protect the state.
	lock  sync.RWMutex
	state State
}

// TransferLeaderCallbackRequest is fsm callbacks param
type TransferLeaderCallbackRequest struct {
	cluster  *cluster.Cluster
	ctx      context.Context
	dispatch eventdispatch.Dispatch

	oldLeader *clusterpb.Shard
	newLeader *clusterpb.Shard
}

func NewTransferLeaderProcedure(dispatch eventdispatch.Dispatch, cluster *cluster.Cluster, oldLeader *clusterpb.Shard, newLeader *clusterpb.Shard, id uint64) Procedure {
	transferLeaderOperationFsm := fsm.NewFSM(
		stateTransferLeaderBegin,
		transferLeaderEvents,
		transferLeaderCallbacks,
	)

	return &TransferLeaderProcedure{id: id, fsm: transferLeaderOperationFsm, dispatch: dispatch, cluster: cluster, oldLeader: oldLeader, newLeader: newLeader, state: StateInit}
}

func (p *TransferLeaderProcedure) ID() uint64 {
	return p.id
}

func (p *TransferLeaderProcedure) Typ() Typ {
	return TransferLeader
}

func (p *TransferLeaderProcedure) Start(ctx context.Context) error {
	p.updateStateWithLock(StateRunning)

	transferLeaderRequest := &TransferLeaderCallbackRequest{
		cluster:   p.cluster,
		ctx:       ctx,
		newLeader: p.newLeader,
		oldLeader: p.oldLeader,
		dispatch:  p.dispatch,
	}

	if err := p.fsm.Event(eventTransferLeaderPrepare, transferLeaderRequest); err != nil {
		err1 := p.fsm.Event(eventTransferLeaderFailed, transferLeaderRequest)
		p.updateStateWithLock(StateFailed)
		if err1 != nil {
			err = errors.WithMessagef(err, "transferLeader procedure start, fail to send eventTransferLeaderFailed err:%v", err1)
		}
		return errors.WithMessage(err, "transferLeader procedure start")
	}

	if err := p.fsm.Event(eventTransferLeaderSuccess, transferLeaderRequest); err != nil {
		return errors.WithMessage(err, "transferLeader procedure start")
	}

	p.updateStateWithLock(StateFinished)
	return nil
}

func (p *TransferLeaderProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(StateCancelled)
	return nil
}

func (p *TransferLeaderProcedure) State() State {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.state
}

func transferLeaderPrepareCallback(event *fsm.Event) {
	request := event.Args[0].(*TransferLeaderCallbackRequest)
	ctx := request.ctx

	closeShardRequest := &eventdispatch.CloseShardRequest{
		ShardID: request.oldLeader.Id,
	}
	if err := request.dispatch.CloseShard(ctx, request.oldLeader.Node, closeShardRequest); err != nil {
		cancelEventWithLog(event, err, "close shard failed", zap.Uint32("shardId", request.oldLeader.Id))
		return
	}

	openShardRequest := &eventdispatch.OpenShardRequest{
		Shard: &cluster.ShardInfo{ID: request.newLeader.Id, Role: clusterpb.ShardRole_LEADER},
	}
	if err := request.dispatch.OpenShard(ctx, request.newLeader.Node, openShardRequest); err != nil {
		cancelEventWithLog(event, err, "open shard failed", zap.Uint32("shardId", request.newLeader.Id))
		return
	}
}

func transferLeaderFailedCallback(_ *fsm.Event) {
	// TODO: Use RollbackProcedure to rollback transfer failed
}

func transferLeaderSuccessCallback(event *fsm.Event) {
	request := event.Args[0].(*TransferLeaderCallbackRequest)
	c := request.cluster
	ctx := request.ctx

	// Update cluster topology
	shardView, err := c.GetClusterShardView()
	if err != nil {
		cancelEventWithLog(event, err, "get cluster shard view failed")
		return
	}
	oldLeaderIndex := -1
	for i := 0; i < len(shardView); i++ {
		shardID := shardView[i].Id
		if shardID == request.oldLeader.Id {
			oldLeaderIndex = i
		}
	}
	if oldLeaderIndex == -1 {
		event.Cancel(errors.WithMessage(cluster.ErrShardNotFound, fmt.Sprintf("shard not found,shardID = %d", request.oldLeader.Id)))
		return
	}
	shardView = append(shardView[:oldLeaderIndex], shardView[oldLeaderIndex+1:]...)
	shardView = append(shardView, request.newLeader)

	if err := c.UpdateClusterTopology(ctx, c.GetClusterState(), shardView); err != nil {
		cancelEventWithLog(event, err, "update shard topology failed")
		return
	}
}

func (p *TransferLeaderProcedure) updateStateWithLock(state State) {
	p.lock.Lock()
	p.state = state
	p.lock.Unlock()
}
