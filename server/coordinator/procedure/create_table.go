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
	table, exists, err := req.cluster.GetTable(req.ctx, req.sourceReq.GetSchemaName(), req.sourceReq.GetName())
	if err != nil {
		cancelEventWithLog(event, err, "cluster get table")
		return
	}
	if exists {
		req.createTableResult = &cluster.CreateTableResult{
			Table: table,
			ShardVersionUpdate: &cluster.ShardVersionUpdate{
				ShardID: table.GetShardID(),
			},
		}
		log.Warn("create an existing table", zap.String("schema", req.sourceReq.GetSchemaName()), zap.String("table", req.sourceReq.GetName()))
		return
	}

	createTableResult, err := req.cluster.CreateTable(req.ctx, req.sourceReq.GetHeader().GetNode(), req.sourceReq.GetSchemaName(), req.sourceReq.GetName())
	if err != nil {
		cancelEventWithLog(event, err, "cluster get table")
		return
	}

	shard, err := req.cluster.GetShardByID(createTableResult.Table.GetShardID())
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
				ID: createTableResult.ShardVersionUpdate.ShardID,
				// TODO: dispatch CreateTableOnShard to followers?
				Role:    clusterpb.ShardRole_LEADER,
				Version: createTableResult.ShardVersionUpdate.CurrVersion,
			},
			PrevVersion: createTableResult.ShardVersionUpdate.PrevVersion,
		},
		TableInfo: &cluster.TableInfo{
			ID:         createTableResult.Table.GetID(),
			Name:       createTableResult.Table.GetName(),
			SchemaID:   createTableResult.Table.GetSchemaID(),
			SchemaName: createTableResult.Table.GetSchemaName(),
		},
		EncodedSchema:    req.sourceReq.EncodedSchema,
		Engine:           req.sourceReq.Engine,
		CreateIfNotExist: req.sourceReq.CreateIfNotExist,
		Options:          req.sourceReq.Options,
	})
	if err != nil {
		cancelEventWithLog(event, err, "dispatch create table on shard")
		return
	}

	req.createTableResult = createTableResult
}

func createTableSuccessCallback(event *fsm.Event) {
	req := event.Args[0].(*createTableCallbackRequest)

	if err := req.onSucceeded(req.createTableResult); err != nil {
		log.Error("exec success callback failed")
	}
}

func createTableFailedCallback(event *fsm.Event) {
	req := event.Args[0].(*createTableCallbackRequest)

	if err := req.onFailed(event.Err); err != nil {
		log.Error("exec failed callback failed")
	}

	table, exists, err := req.cluster.GetTable(req.ctx, req.sourceReq.GetSchemaName(), req.sourceReq.GetName())
	if err != nil {
		log.Error("create table failed, get table failed", zap.String("schemaName", req.sourceReq.GetSchemaName()), zap.String("tableName", req.sourceReq.GetName()))
		return
	}
	if !exists {
		return
	}

	// Rollback, drop table in ceresmeta.
	_, err = req.cluster.DropTable(req.ctx, table.GetSchemaName(), table.GetName())
	if err != nil {
		log.Error("drop table failed, get table failed", zap.String("schemaName", table.GetSchemaName()), zap.String("tableName", table.GetName()), zap.Uint64("tableID", table.GetID()))
		return
	}
}

// createTableCallbackRequest is fsm callbacks param.
type createTableCallbackRequest struct {
	ctx      context.Context
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	sourceReq *metaservicepb.CreateTableRequest

	onSucceeded func(*cluster.CreateTableResult) error
	onFailed    func(error) error

	createTableResult *cluster.CreateTableResult
}

func NewCreateTableProcedure(dispatch eventdispatch.Dispatch, cluster *cluster.Cluster, id uint64, req *metaservicepb.CreateTableRequest, onSucceeded func(*cluster.CreateTableResult) error, onFailed func(error) error) Procedure {
	fsm := fsm.NewFSM(
		stateCreateTableBegin,
		createTableEvents,
		createTableCallbacks,
	)
	return &CreateTableProcedure{id: id, fsm: fsm, cluster: cluster, dispatch: dispatch, req: req, state: StateInit, onSucceeded: onSucceeded, onFailed: onFailed}
}

type CreateTableProcedure struct {
	id       uint64
	fsm      *fsm.FSM
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	req *metaservicepb.CreateTableRequest

	onSucceeded func(*cluster.CreateTableResult) error
	onFailed    func(error) error

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
