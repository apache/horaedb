// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
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
	req := event.Args[0].(*createTableCallbackRequest)
	_, exists, err := req.cluster.GetTable(req.ctx, req.rawReq.GetSchemaName(), req.rawReq.GetName())
	if err != nil {
		cancelEventWithLog(event, err, "cluster get table")
		return
	}
	if exists {
		log.Warn("create an existing table", zap.String("schema", req.rawReq.GetSchemaName()), zap.String("table", req.rawReq.GetName()))
		return
	}

	ret, err := req.cluster.CreateTable(req.ctx, req.rawReq.GetHeader().GetNode(), req.rawReq.GetSchemaName(), req.rawReq.GetName())
	if err != nil {
		cancelEventWithLog(event, err, "cluster get table")
		return
	}

	shard, err := req.cluster.GetShardByID(ret.Table.GetShardID())
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

	err = req.dispatch.CreateTableOnShard(req.ctx, leader.Node, &eventdispatch.CreateTableOnShardRequest{
		UpdateShardInfo: &eventdispatch.UpdateShardInfo{
			CurrShardInfo: &cluster.ShardInfo{
				ID: ret.ID,
				// TODO: dispatch CreateTableOnShard to followers?
				Role:    clusterpb.ShardRole_LEADER,
				Version: ret.CurrVersion,
			},
			PrevVersion: ret.PrevVersion,
		},
		TableInfo: &cluster.TableInfo{
			ID:         ret.Table.GetID(),
			Name:       ret.Table.GetName(),
			SchemaID:   ret.Table.GetSchemaID(),
			SchemaName: ret.Table.GetSchemaName(),
		},
		EncodedSchema:    req.rawReq.EncodedSchema,
		Engine:           req.rawReq.Engine,
		CreateIfNotExist: req.rawReq.CreateIfNotExist,
		Options:          req.rawReq.Options,
	})
	if err != nil {
		cancelEventWithLog(event, err, "dispatch create table on shard")
		return
	}
}

func createTableSuccessCallback(event *fsm.Event) {
	req := event.Args[0].(*createTableCallbackRequest)

	if err := req.onSuccess(); err != nil {
		log.Error("exec success callback failed")
	}
}

func createTableFailedCallback(event *fsm.Event) {
	req := event.Args[0].(*createTableCallbackRequest)

	table, exists, err := req.cluster.GetTable(req.ctx, req.rawReq.GetSchemaName(), req.rawReq.GetName())
	if err != nil {
		log.Error("create table failed, get table failed", zap.String("schemaName", req.rawReq.GetSchemaName()), zap.String("tableName", req.rawReq.GetName()))
		return
	}
	if !exists {
		return
	}

	// Rollback, drop table in ceresmeta.
	err = req.cluster.DropTable(req.ctx, table.GetSchemaName(), table.GetName(), table.GetID())
	if err != nil {
		log.Error("drop table failed, get table failed", zap.String("schemaName", table.GetSchemaName()), zap.String("tableName", table.GetName()), zap.Uint64("tableID", table.GetID()))
		return
	}
	if err := req.onFailed(); err != nil {
		log.Error("exec success callback failed")
	}
}

// createTableCallbackRequest is fsm callbacks param.
type createTableCallbackRequest struct {
	ctx      context.Context
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	rawReq *metaservicepb.CreateTableRequest

	// TODO: correct callback input params
	onSuccess func() error
	onFailed  func() error
}

func NewCreateTableProcedure(dispatch eventdispatch.Dispatch, cluster *cluster.Cluster, id uint64, req *metaservicepb.CreateTableRequest, onSuccess func() error, onFailed func() error) Procedure {
	fsm := fsm.NewFSM(
		stateCreateTableBegin,
		createTableEvents,
		createTableCallbacks,
	)
	return &CreateTableProcedure{id: id, fsm: fsm, cluster: cluster, dispatch: dispatch, req: req, state: StateInit, onSuccess: onSuccess, onFailed: onFailed}
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
	p.updateState(StateRunning)

	req := &createTableCallbackRequest{
		cluster:   p.cluster,
		ctx:       ctx,
		dispatch:  p.dispatch,
		rawReq:    p.req,
		onSuccess: p.onSuccess,
		onFailed:  p.onFailed,
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
