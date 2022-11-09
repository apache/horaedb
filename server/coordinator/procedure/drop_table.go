// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"fmt"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
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
	request := event.Args[0].(*dropTableCallbackRequest)
	table, exists, err := request.cluster.GetTable(request.rawReq.GetSchemaName(), request.rawReq.GetName())
	if err != nil {
		cancelEventWithLog(event, err, "cluster get table")
		return
	}
	if !exists {
		log.Warn("drop non-existing table", zap.String("schema", request.rawReq.GetSchemaName()), zap.String("table", request.rawReq.GetName()))
		return
	}
	result, err := request.cluster.DropTable(request.ctx, request.rawReq.GetSchemaName(), request.rawReq.GetName())
	if err != nil {
		cancelEventWithLog(event, err, "cluster drop table")
		return
	}

	shardNodesResult, err := request.cluster.GetShardNodeByTableIDs([]storage.TableID{table.ID})
	if err != nil {
		cancelEventWithLog(event, err, "cluster get shard by table id")
		return
	}

	shardNodes, ok := shardNodesResult.ShardNodes[table.ID]
	if !ok {
		cancelEventWithLog(event, ErrShardLeaderNotFound, fmt.Sprintf("cluster get shard by table id, table:%v", table))
		return
	}

	// TODO: consider followers
	leader := storage.ShardNode{}
	found := false
	for _, shardNode := range shardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			found = true
			leader = shardNode
			break
		}
	}

	if !found {
		cancelEventWithLog(event, ErrShardLeaderNotFound, "can't find leader")
		return
	}

	tableInfo := cluster.TableInfo{
		ID:         table.ID,
		Name:       table.Name,
		SchemaID:   table.SchemaID,
		SchemaName: request.rawReq.GetSchemaName(),
	}
	err = request.dispatch.DropTableOnShard(request.ctx, leader.NodeName, eventdispatch.DropTableOnShardRequest{
		UpdateShardInfo: eventdispatch.UpdateShardInfo{
			CurrShardInfo: cluster.ShardInfo{
				ID:      result.ShardVersionUpdate.ShardID,
				Role:    storage.ShardRoleLeader,
				Version: result.ShardVersionUpdate.CurrVersion,
			},
			PrevVersion: result.ShardVersionUpdate.PrevVersion,
		},
		TableInfo: tableInfo,
	})
	if err != nil {
		cancelEventWithLog(event, err, "dispatch drop table on shard")
		return
	}

	request.ret = tableInfo
}

func dropTableSuccessCallback(event *fsm.Event) {
	req := event.Args[0].(*dropTableCallbackRequest)

	if err := req.onSucceeded(req.ret); err != nil {
		log.Error("exec success callback failed")
	}
}

func dropTableFailedCallback(event *fsm.Event) {
	req := event.Args[0].(*dropTableCallbackRequest)

	if err := req.onFailed(event.Err); err != nil {
		log.Error("exec failed callback failed")
	}
}

// dropTableCallbackRequest is fsm callbacks param.
type dropTableCallbackRequest struct {
	ctx      context.Context
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	rawReq *metaservicepb.DropTableRequest

	onSucceeded func(cluster.TableInfo) error
	onFailed    func(error) error

	ret cluster.TableInfo
}

func NewDropTableProcedure(dispatch eventdispatch.Dispatch, cluster *cluster.Cluster, id uint64, req *metaservicepb.DropTableRequest, onSucceeded func(cluster.TableInfo) error, onFailed func(error) error) Procedure {
	fsm := fsm.NewFSM(
		stateDropTableBegin,
		dropTableEvents,
		dropTableCallbacks,
	)
	return &DropTableProcedure{id: id, fsm: fsm, cluster: cluster, dispatch: dispatch, req: req, onSucceeded: onSucceeded, onFailed: onFailed, state: StateInit}
}

type DropTableProcedure struct {
	id       uint64
	fsm      *fsm.FSM
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch
	req      *metaservicepb.DropTableRequest

	onSucceeded func(cluster.TableInfo) error
	onFailed    func(error) error

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
	p.updateState(StateRunning)

	req := &dropTableCallbackRequest{
		cluster:     p.cluster,
		ctx:         ctx,
		dispatch:    p.dispatch,
		rawReq:      p.req,
		onSucceeded: p.onSucceeded,
		onFailed:    p.onFailed,
	}

	if err := p.fsm.Event(eventDropTablePrepare, req); err != nil {
		err1 := p.fsm.Event(eventDropTableFailed, req)
		p.updateState(StateFailed)
		if err1 != nil {
			err = errors.WithMessagef(err, "send eventDropTableFailed, err:%v", err1)
		}
		return errors.WithMessage(err, "send eventDropTablePrepare")
	}

	if err := p.fsm.Event(eventDropTableSuccess, req); err != nil {
		return errors.WithMessage(err, "send eventDropTableSuccess")
	}

	p.updateState(StateFinished)
	return nil
}

func (p *DropTableProcedure) Cancel(_ context.Context) error {
	p.updateState(StateCancelled)
	return nil
}

func (p *DropTableProcedure) State() State {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.state
}

func (p *DropTableProcedure) updateState(state State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}
