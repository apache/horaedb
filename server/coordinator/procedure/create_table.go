// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
)

const (
	eventCreateTablePrepare = "EventCreateTablePrepare"
	eventCreateTableFailed  = "EventCreateTableFailed"
	eventCreateTableSuccess = "EventCreateTableSuccess"

	stateCreateTableBegin   = "StateCreateTableBegin"
	stateCreateTableWaiting = "StateCreateTableWaiting"
	stateCreateTableFinish  = "StateCreateTableFinish"
	stateCreateTableFailed  = "StateCreateTableFailed"
)

var (
	createTableEvents = fsm.Events{
		{Name: eventCreateTablePrepare, Src: []string{stateCreateTableBegin}, Dst: stateCreateTableWaiting},
		{Name: eventCreateTableSuccess, Src: []string{stateCreateTableWaiting}, Dst: stateCreateTableFinish},
		{Name: eventCreateTableFailed, Src: []string{stateCreateTableWaiting}, Dst: stateCreateTableFailed},
	}
	createTableCallbacks = fsm.Callbacks{
		eventCreateTablePrepare: createTablePrepareCallback,
		eventCreateTableFailed:  createTableFailedCallback,
		eventCreateTableSuccess: createTableSuccessCallback,
	}
)

func createTablePrepareCallback(event *fsm.Event) {
	req, err := getRequestFromEvent[*createTableCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	createTableResult, err := createTableMetadata(req.ctx, req.cluster, req.sourceReq.GetSchemaName(), req.sourceReq.GetName(), req.shardID, false)
	if err != nil {
		cancelEventWithLog(event, err, "create table metadata")
		return
	}

	if err = createTableOnShard(req.ctx, req.cluster, req.dispatch, createTableResult.ShardVersionUpdate.ShardID, buildCreateTableRequest(createTableResult, req.sourceReq, false)); err != nil {
		cancelEventWithLog(event, err, "dispatch create table on shard")
		return
	}

	req.createTableResult = createTableResult
}

func createTableSuccessCallback(event *fsm.Event) {
	req, err := getRequestFromEvent[*createTableCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	if err := req.onSucceeded(req.createTableResult); err != nil {
		log.Error("exec success callback failed")
	}
}

func createTableFailedCallback(event *fsm.Event) {
	req, err := getRequestFromEvent[*createTableCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	if err := req.onFailed(event.Err); err != nil {
		log.Error("exec failed callback failed")
	}
}

// createTableCallbackRequest is fsm callbacks param.
type createTableCallbackRequest struct {
	ctx      context.Context
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	sourceReq *metaservicepb.CreateTableRequest
	shardID   storage.ShardID

	onSucceeded func(cluster.CreateTableResult) error
	onFailed    func(error) error

	createTableResult cluster.CreateTableResult
}

type CreateTableProcedureRequest struct {
	Dispatch    eventdispatch.Dispatch
	Cluster     *cluster.Cluster
	ID          uint64
	ShardID     storage.ShardID
	Req         *metaservicepb.CreateTableRequest
	OnSucceeded func(cluster.CreateTableResult) error
	OnFailed    func(error) error
}

func NewCreateTableProcedure(request CreateTableProcedureRequest) Procedure {
	fsm := fsm.NewFSM(
		stateCreateTableBegin,
		createTableEvents,
		createTableCallbacks,
	)
	return &CreateTableProcedure{
		id:          request.ID,
		fsm:         fsm,
		cluster:     request.Cluster,
		dispatch:    request.Dispatch,
		shardID:     request.ShardID,
		req:         request.Req,
		state:       StateInit,
		onSucceeded: request.OnSucceeded,
		onFailed:    request.OnFailed,
	}
}

type CreateTableProcedure struct {
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
	state State
}

func (p *CreateTableProcedure) ID() uint64 {
	return p.id
}

func (p *CreateTableProcedure) Typ() Typ {
	return CreateTable
}

func (p *CreateTableProcedure) Start(ctx context.Context) error {
	p.updateState(StateRunning)

	req := &createTableCallbackRequest{
		cluster:     p.cluster,
		ctx:         ctx,
		dispatch:    p.dispatch,
		shardID:     p.shardID,
		sourceReq:   p.req,
		onSucceeded: p.onSucceeded,
		onFailed:    p.onFailed,
	}

	if err := p.fsm.Event(eventCreateTablePrepare, req); err != nil {
		err1 := p.fsm.Event(eventCreateTableFailed, req)
		p.updateState(StateFailed)
		if err1 != nil {
			err = errors.WithMessagef(err, "send eventCreateTableFailed, err:%v", err1)
		}
		return errors.WithMessage(err, "send eventCreateTablePrepare")
	}

	if err := p.fsm.Event(eventCreateTableSuccess, req); err != nil {
		return errors.WithMessage(err, "send eventCreateTableSuccess")
	}

	p.updateState(StateFinished)
	return nil
}

func (p *CreateTableProcedure) Cancel(_ context.Context) error {
	p.updateState(StateCancelled)
	return nil
}

func (p *CreateTableProcedure) State() State {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.state
}

func (p *CreateTableProcedure) updateState(state State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}
