// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/CeresDB/ceresmeta/pkg/log"

	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
)

const (
	eventDropTablePrepare = "EventDropTablePrepare"
	eventDropTableFailed  = "EventDropTableFailed"
	eventDropTableSuccess = "EventDropTableSuccess"

	stateDropTableBegin   = "StateDropTableBegin"
	stateDropTableWaiting = "StateDropTableWaiting"
	stateDropTableFinish  = "StateDropTableFinish"
	stateDropTableFailed  = "StateDropTableFailed"
)

var (
	dropTableEvents = fsm.Events{
		{Name: eventDropTablePrepare, Src: []string{stateDropTableBegin}, Dst: stateDropTableWaiting},
		{Name: eventDropTableSuccess, Src: []string{stateDropTableWaiting}, Dst: stateDropTableFinish},
		{Name: eventDropTableFailed, Src: []string{stateDropTableWaiting}, Dst: stateDropTableFailed},
	}
	dropTableCallbacks = fsm.Callbacks{
		eventDropTablePrepare: dropTablePrepareCallback,
		eventDropTableFailed:  dropTableFailedCallback,
		eventDropTableSuccess: dropTableSuccessCallback,
	}
)

func dropTablePrepareCallback(event *fsm.Event) {
	request := event.Args[0].(*DropTableCallbackRequest)
	table, exists, err := request.cluster.GetTable(request.ctx, request.schemaName, request.tableName)
	if err != nil {
		cancelEventWithLog(event, err, "cluster get table")
		return
	}
	if !exists {
		log.Warn("drop non-existing table", zap.String("schema", request.schemaName), zap.String("table", request.tableName), zap.Uint64("tableID", request.tableID))
		return
	}
	err = request.cluster.DropTable(request.ctx, request.schemaName, request.tableName, request.tableID)
	if err != nil {
		cancelEventWithLog(event, err, "cluster drop table")
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
		cancelEventWithLog(event, ErrShardLeaderNotFound, "can't find leader")
		return
	}

	err = request.dispatch.DropTableOnShard(request.ctx, leader.Node, &eventdispatch.DropTableOnShardRequest{
		TableInfo: &cluster.TableInfo{
			ID:         table.GetID(),
			Name:       table.GetName(),
			SchemaID:   table.GetSchemaID(),
			SchemaName: table.GetSchemaName(),
		},
	})
	if err != nil {
		cancelEventWithLog(event, err, "dispatch drop table on shard")
		return
	}
}

func dropTableSuccessCallback(_ *fsm.Event) {
}

func dropTableFailedCallback(_ *fsm.Event) {
	// TODO: Use RollbackProcedure to rollback transfer failed
}

// DropTableCallbackRequest is fsm callbacks param.
type DropTableCallbackRequest struct {
	ctx      context.Context
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	schemaName string
	tableName  string
	tableID    uint64
}

func NewDropTableProcedure(dispatch eventdispatch.Dispatch, cluster *cluster.Cluster, id uint64, req *metaservicepb.DropTableRequest) Procedure {
	dropTableProcedureFsm := fsm.NewFSM(
		stateDropTableBegin,
		dropTableEvents,
		dropTableCallbacks,
	)
	return &DropTableProcedure{id: id, fsm: dropTableProcedureFsm, cluster: cluster, dispatch: dispatch, req: req, state: StateInit}
}

type DropTableProcedure struct {
	id       uint64
	fsm      *fsm.FSM
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch
	req      *metaservicepb.DropTableRequest

	lock  sync.RWMutex
	state State
}

func (p *DropTableProcedure) ID() uint64 {
	return p.id
}

func (p *DropTableProcedure) Typ() Typ {
	return DropTable
}

func (p *DropTableProcedure) Start(ctx context.Context) error {
	p.updateStateWithLock(StateRunning)

	dropTableCallbackRequest := &DropTableCallbackRequest{
		cluster:    p.cluster,
		ctx:        ctx,
		dispatch:   p.dispatch,
		schemaName: p.req.GetSchemaName(),
		tableName:  p.req.GetName(),
		tableID:    p.req.GetId(),
	}

	if err := p.fsm.Event(eventDropTablePrepare, dropTableCallbackRequest); err != nil {
		err1 := p.fsm.Event(eventDropTableFailed, dropTableCallbackRequest)
		p.updateStateWithLock(StateFailed)
		if err1 != nil {
			err = errors.WithMessagef(err, "dropTable procedure start, fail to send eventDropTableFailed err:%v", err1)
		}
		return errors.WithMessage(err, "dropTable procedure start")
	}

	if err := p.fsm.Event(eventDropTableSuccess, dropTableCallbackRequest); err != nil {
		return errors.WithMessage(err, "dropTable procedure start")
	}

	p.updateStateWithLock(StateFinished)
	return nil
}

func (p *DropTableProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(StateCancelled)
	return nil
}

func (p *DropTableProcedure) State() State {
	return p.state
}

func (p *DropTableProcedure) updateStateWithLock(state State) {
	p.lock.Lock()
	p.state = state
	p.lock.Unlock()
}
