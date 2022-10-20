// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
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
	request := event.Args[0].(*CreateTableCallbackRequest)
	table, exists, err := request.cluster.GetOrCreateTable(request.ctx, request.nodeName, request.schemaName, request.tableName)
	if err != nil {
		cancelEventWithLog(event, err, "cluster get or create table")
		return
	}
	if exists {
		log.Warn("create an existing table", zap.String("schema", request.schemaName), zap.String("table", request.tableName))
		return
	}

	shard, err := request.cluster.GetShardByID(table.GetShardID())
	if err != nil {
		cancelEventWithLog(event, err, "cluster get shard by id")
		return
	}
	// TODO: consider followers
	leader := shard.GetLeader()
	if leader == nil {
		cancelEventWithLog(event, ErrShardLeaderNotFound, "shard can't find leader")
		return
	}

	err = request.dispatch.CreateTableOnShard(request.ctx, leader.Node, &eventdispatch.CreateTableOnShardRequest{
		TableInfo: &cluster.TableInfo{
			ID:         table.GetID(),
			Name:       table.GetName(),
			SchemaID:   table.GetSchemaID(),
			SchemaName: table.GetSchemaName(),
		},
		CreateSQL: request.createSQL,
	})
	if err != nil {
		cancelEventWithLog(event, err, "dispatch create table on shard")
		return
	}
}

func createTableSuccessCallback(event *fsm.Event) {
	request := event.Args[0].(*CreateTableCallbackRequest)

	if err := request.onSuccess(); err != nil {
		log.Error("exec success callback failed")
	}
}

func createTableFailedCallback(event *fsm.Event) {
	request := event.Args[0].(*CreateTableCallbackRequest)

	if err := request.onFailed(); err != nil {
		log.Error("exec success callback failed")
	}
}

// CreateTableCallbackRequest is fsm callbacks param.
type CreateTableCallbackRequest struct {
	ctx      context.Context
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	schemaName string
	tableName  string
	nodeName   string
	createSQL  string

	// TODO: correct callback input params
	onSuccess func() error
	onFailed  func() error
}

func NewCreateTableProcedure(dispatch eventdispatch.Dispatch, cluster *cluster.Cluster, id uint64, req *metaservicepb.CreateTableRequest, onSuccess func() error, onFailed func() error) Procedure {
	createTableProcedureFsm := fsm.NewFSM(
		stateCreateTableBegin,
		createTableEvents,
		createTableCallbacks,
	)
	return &CreateTableProcedure{id: id, fsm: createTableProcedureFsm, cluster: cluster, dispatch: dispatch, req: req, state: StateInit, onSuccess: onSuccess, onFailed: onFailed}
}

type CreateTableProcedure struct {
	id       uint64
	fsm      *fsm.FSM
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch
	req      *metaservicepb.CreateTableRequest
	// TODO: correct callback input params
	onSuccess func() error
	onFailed  func() error

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
	p.updateStateWithLock(StateRunning)

	createTableCallbackRequest := &CreateTableCallbackRequest{
		cluster:    p.cluster,
		ctx:        ctx,
		dispatch:   p.dispatch,
		tableName:  p.req.GetName(),
		schemaName: p.req.GetSchemaName(),
		nodeName:   p.req.GetHeader().GetNode(),
		createSQL:  p.req.GetCreateSql(),
	}

	if err := p.fsm.Event(eventCreateTablePrepare, createTableCallbackRequest); err != nil {
		err1 := p.fsm.Event(eventCreateTableFailed, createTableCallbackRequest)
		p.updateStateWithLock(StateFailed)
		if err1 != nil {
			err = errors.WithMessagef(err, "createTable procedure start, fail to send eventCreateTableFailed err:%v", err1)
		}
		return errors.WithMessage(err, "createTable procedure start")
	}

	if err := p.fsm.Event(eventCreateTableSuccess, createTableCallbackRequest); err != nil {
		return errors.WithMessage(err, "createTable procedure start")
	}

	p.updateStateWithLock(StateFinished)
	return nil
}

func (p *CreateTableProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(StateCancelled)
	return nil
}

func (p *CreateTableProcedure) State() State {
	return p.state
}

func (p *CreateTableProcedure) updateStateWithLock(state State) {
	p.lock.Lock()
	p.state = state
	p.lock.Unlock()
}
