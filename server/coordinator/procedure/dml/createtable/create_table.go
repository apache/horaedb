// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package createtable

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
)

const (
	eventPrepare = "EventPrepare"
	eventFailed  = "EventFailed"
	eventSuccess = "EventSuccess"

	stateBegin   = "StateBegin"
	stateWaiting = "StateWaiting"
	stateFinish  = "StateFinish"
	stateFailed  = "StateFailed"
)

var (
	createTableEvents = fsm.Events{
		{Name: eventPrepare, Src: []string{stateBegin}, Dst: stateWaiting},
		{Name: eventSuccess, Src: []string{stateWaiting}, Dst: stateFinish},
		{Name: eventFailed, Src: []string{stateWaiting}, Dst: stateFailed},
	}
	createTableCallbacks = fsm.Callbacks{
		eventPrepare: prepareCallback,
		eventFailed:  failedCallback,
		eventSuccess: successCallback,
	}
)

func prepareCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	createTableResult, err := procedure.CreateTableMetadata(req.ctx, req.cluster, req.sourceReq.GetSchemaName(), req.sourceReq.GetName(), req.shardID, nil)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "create table metadata")
		return
	}

	if err = procedure.CreateTableOnShard(req.ctx, req.cluster, req.dispatch, createTableResult.ShardVersionUpdate.ShardID, procedure.BuildCreateTableRequest(createTableResult, req.sourceReq, req.sourceReq.GetPartitionTableInfo().GetPartitionInfo())); err != nil {
		procedure.CancelEventWithLog(event, err, "dispatch create table on shard")
		return
	}

	req.createTableResult = createTableResult
}

func successCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	if err := req.onSucceeded(req.createTableResult); err != nil {
		log.Error("exec success callback failed")
	}
}

func failedCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	if err := req.onFailed(event.Err); err != nil {
		log.Error("exec failed callback failed")
	}
}

// callbackRequest is fsm callbacks param.
type callbackRequest struct {
	ctx      context.Context
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	sourceReq *metaservicepb.CreateTableRequest
	shardID   storage.ShardID

	onSucceeded func(cluster.CreateTableResult) error
	onFailed    func(error) error

	createTableResult cluster.CreateTableResult
}

type ProcedureRequest struct {
	Dispatch    eventdispatch.Dispatch
	Cluster     *cluster.Cluster
	ID          uint64
	ShardID     storage.ShardID
	Req         *metaservicepb.CreateTableRequest
	OnSucceeded func(cluster.CreateTableResult) error
	OnFailed    func(error) error
}

func NewProcedure(req ProcedureRequest) procedure.Procedure {
	fsm := fsm.NewFSM(
		stateBegin,
		createTableEvents,
		createTableCallbacks,
	)
	return &Procedure{
		id:          req.ID,
		fsm:         fsm,
		cluster:     req.Cluster,
		dispatch:    req.Dispatch,
		shardID:     req.ShardID,
		req:         req.Req,
		state:       procedure.StateInit,
		onSucceeded: req.OnSucceeded,
		onFailed:    req.OnFailed,
	}
}

type Procedure struct {
	id       uint64
	fsm      *fsm.FSM
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	shardID storage.ShardID

	req *metaservicepb.CreateTableRequest

	onSucceeded func(cluster.CreateTableResult) error
	onFailed    func(error) error

	// Protect the state.
	lock  sync.RWMutex
	state procedure.State
}

func (p *Procedure) ID() uint64 {
	return p.id
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.CreateTable
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateState(procedure.StateRunning)

	req := &callbackRequest{
		cluster:     p.cluster,
		ctx:         ctx,
		dispatch:    p.dispatch,
		shardID:     p.shardID,
		sourceReq:   p.req,
		onSucceeded: p.onSucceeded,
		onFailed:    p.onFailed,
	}

	if err := p.fsm.Event(eventPrepare, req); err != nil {
		err1 := p.fsm.Event(eventFailed, req)
		p.updateState(procedure.StateFailed)
		if err1 != nil {
			err = errors.WithMessagef(err, "send eventFailed, err:%v", err1)
		}
		return errors.WithMessage(err, "send eventPrepare")
	}

	if err := p.fsm.Event(eventSuccess, req); err != nil {
		return errors.WithMessage(err, "send eventSuccess")
	}

	p.updateState(procedure.StateFinished)
	return nil
}

func (p *Procedure) Cancel(_ context.Context) error {
	p.updateState(procedure.StateCancelled)
	return nil
}

func (p *Procedure) State() procedure.State {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.state
}

func (p *Procedure) updateState(state procedure.State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}
