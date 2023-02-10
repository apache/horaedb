// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package droptable

import (
	"context"
	"fmt"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
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
	dropTableEvents = fsm.Events{
		{Name: eventPrepare, Src: []string{stateBegin}, Dst: stateWaiting},
		{Name: eventSuccess, Src: []string{stateWaiting}, Dst: stateFinish},
		{Name: eventFailed, Src: []string{stateWaiting}, Dst: stateFailed},
	}
	dropTableCallbacks = fsm.Callbacks{
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

	table, exists, err := req.cluster.GetTable(req.rawReq.GetSchemaName(), req.rawReq.GetName())
	if err != nil {
		procedure.CancelEventWithLog(event, err, "cluster get table")
		return
	}
	if !exists {
		log.Warn("drop non-existing table", zap.String("schema", req.rawReq.GetSchemaName()), zap.String("table", req.rawReq.GetName()))
		return
	}

	shardNodesResult, err := req.cluster.GetShardNodeByTableIDs([]storage.TableID{table.ID})
	if err != nil {
		procedure.CancelEventWithLog(event, err, "cluster get shard by table id")
		return
	}

	result, err := req.cluster.DropTable(req.ctx, req.rawReq.GetSchemaName(), req.rawReq.GetName())
	if err != nil {
		procedure.CancelEventWithLog(event, err, "cluster drop table")
		return
	}

	if len(result.ShardVersionUpdate) != 1 {
		procedure.CancelEventWithLog(event, procedure.ErrDropTableResult, fmt.Sprintf("legnth of shardVersionResult is %d", len(result.ShardVersionUpdate)))
		return
	}

	shardNodes, ok := shardNodesResult.ShardNodes[table.ID]
	if !ok {
		procedure.CancelEventWithLog(event, procedure.ErrShardLeaderNotFound, fmt.Sprintf("cluster get shard by table id, table:%v", table))
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
		procedure.CancelEventWithLog(event, procedure.ErrShardLeaderNotFound, "can't find leader")
		return
	}

	tableInfo := cluster.TableInfo{
		ID:         table.ID,
		Name:       table.Name,
		SchemaID:   table.SchemaID,
		SchemaName: req.rawReq.GetSchemaName(),
	}
	err = req.dispatch.DropTableOnShard(req.ctx, leader.NodeName, eventdispatch.DropTableOnShardRequest{
		UpdateShardInfo: eventdispatch.UpdateShardInfo{
			CurrShardInfo: cluster.ShardInfo{
				ID:      result.ShardVersionUpdate[0].ShardID,
				Role:    storage.ShardRoleLeader,
				Version: result.ShardVersionUpdate[0].CurrVersion,
			},
			PrevVersion: result.ShardVersionUpdate[0].PrevVersion,
		},
		TableInfo: tableInfo,
	})
	if err != nil {
		procedure.CancelEventWithLog(event, err, "dispatch drop table on shard")
		return
	}

	req.ret = tableInfo
}

func successCallback(event *fsm.Event) {
	req := event.Args[0].(*callbackRequest)

	if err := req.onSucceeded(req.ret); err != nil {
		log.Error("exec success callback failed")
	}
}

func failedCallback(event *fsm.Event) {
	req := event.Args[0].(*callbackRequest)

	if err := req.onFailed(event.Err); err != nil {
		log.Error("exec failed callback failed")
	}
}

// callbackRequest is fsm callbacks param.
type callbackRequest struct {
	ctx      context.Context
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	rawReq *metaservicepb.DropTableRequest

	onSucceeded func(cluster.TableInfo) error
	onFailed    func(error) error

	ret cluster.TableInfo
}

func NewDropTableProcedure(dispatch eventdispatch.Dispatch, cluster *cluster.Cluster, id uint64, req *metaservicepb.DropTableRequest, onSucceeded func(cluster.TableInfo) error, onFailed func(error) error) procedure.Procedure {
	fsm := fsm.NewFSM(
		stateBegin,
		dropTableEvents,
		dropTableCallbacks,
	)
	return &Procedure{
		id:          id,
		fsm:         fsm,
		cluster:     cluster,
		dispatch:    dispatch,
		req:         req,
		onSucceeded: onSucceeded,
		onFailed:    onFailed,
		state:       procedure.StateInit,
	}
}

type Procedure struct {
	id       uint64
	fsm      *fsm.FSM
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch
	req      *metaservicepb.DropTableRequest

	onSucceeded func(cluster.TableInfo) error
	onFailed    func(error) error

	lock  sync.RWMutex
	state procedure.State
}

func (p *Procedure) ID() uint64 {
	return p.id
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.DropTable
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateState(procedure.StateRunning)

	req := &callbackRequest{
		cluster:     p.cluster,
		ctx:         ctx,
		dispatch:    p.dispatch,
		rawReq:      p.req,
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
