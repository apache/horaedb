// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package createtable

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/ddl"
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
	params := req.p.params

	createTableMetadataRequest := metadata.CreateTableMetadataRequest{
		SchemaName:    params.SourceReq.GetSchemaName(),
		TableName:     params.SourceReq.GetName(),
		PartitionInfo: storage.PartitionInfo{Info: params.SourceReq.PartitionTableInfo.GetPartitionInfo()},
	}
	result, err := params.ClusterMetadata.CreateTableMetadata(req.ctx, createTableMetadataRequest)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "create table metadata")
		return
	}

	log.Debug("create table metadata finish", zap.String("tableName", createTableMetadataRequest.TableName))

	shardVersionUpdate := metadata.ShardVersionUpdate{
		ShardID:     params.ShardID,
		CurrVersion: req.p.relatedVersionInfo.ShardWithVersion[params.ShardID] + 1,
		PrevVersion: req.p.relatedVersionInfo.ShardWithVersion[params.ShardID],
	}

	createTableRequest := ddl.BuildCreateTableRequest(result.Table, shardVersionUpdate, params.SourceReq)
	if err = ddl.CreateTableOnShard(req.ctx, params.ClusterMetadata, params.Dispatch, params.ShardID, createTableRequest); err != nil {
		procedure.CancelEventWithLog(event, err, "dispatch create table on shard")
		return
	}

	log.Debug("dispatch createTableOnShard finish", zap.String("tableName", createTableMetadataRequest.TableName))

	createTableResult, err := params.ClusterMetadata.AddTableTopology(req.ctx, params.ShardID, result.Table)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "add table topology")
		return
	}

	log.Debug("add table topology finish", zap.String("tableName", createTableMetadataRequest.TableName))

	req.createTableResult = createTableResult
}

func successCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	if err := req.p.params.OnSucceeded(req.createTableResult); err != nil {
		log.Error("exec success callback failed")
	}
}

func failedCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	if err := req.p.params.OnFailed(event.Err); err != nil {
		log.Error("exec failed callback failed")
	}
}

// callbackRequest is fsm callbacks param.
type callbackRequest struct {
	ctx               context.Context
	p                 *Procedure
	createTableResult metadata.CreateTableResult
}

type ProcedureParams struct {
	Dispatch        eventdispatch.Dispatch
	ClusterMetadata *metadata.ClusterMetadata
	ClusterSnapshot metadata.Snapshot
	ID              uint64
	ShardID         storage.ShardID
	SourceReq       *metaservicepb.CreateTableRequest
	OnSucceeded     func(metadata.CreateTableResult) error
	OnFailed        func(error) error
}

func NewProcedure(params ProcedureParams) (procedure.Procedure, error) {
	fsm := fsm.NewFSM(
		stateBegin,
		createTableEvents,
		createTableCallbacks,
	)

	relatedVersionInfo, err := buildRelatedVersionInfo(params)
	if err != nil {
		return nil, err
	}

	return &Procedure{
		fsm:                fsm,
		params:             params,
		relatedVersionInfo: relatedVersionInfo,
		state:              procedure.StateInit,
	}, nil
}

type Procedure struct {
	fsm                *fsm.FSM
	params             ProcedureParams
	relatedVersionInfo procedure.RelatedVersionInfo
	// Protect the state.
	lock  sync.RWMutex
	state procedure.State
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func buildRelatedVersionInfo(params ProcedureParams) (procedure.RelatedVersionInfo, error) {
	shardWithVersion := make(map[storage.ShardID]uint64, 1)
	shardView, exists := params.ClusterSnapshot.Topology.ShardViewsMapping[params.ShardID]
	if !exists {
		return procedure.RelatedVersionInfo{}, errors.WithMessagef(metadata.ErrShardNotFound, "shard not found in topology, shardID:%d", params.ShardID)
	}
	shardWithVersion[params.ShardID] = shardView.Version
	return procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapshot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardWithVersion,
		ClusterVersion:   params.ClusterSnapshot.Topology.ClusterView.Version,
	}, nil
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityLow
}

func (p *Procedure) ID() uint64 {
	return p.params.ID
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.CreateTable
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateState(procedure.StateRunning)

	req := &callbackRequest{
		ctx: ctx,
		p:   p,
	}

	if err := p.fsm.Event(eventPrepare, req); err != nil {
		err1 := p.fsm.Event(eventFailed, req)
		p.updateState(procedure.StateFailed)
		if err1 != nil {
			err = errors.WithMessagef(err, "send eventFailed, err:%v", err1)
		}
		_ = p.params.OnFailed(err)
		return errors.WithMessage(err, "send eventPrepare")
	}

	if err := p.fsm.Event(eventSuccess, req); err != nil {
		_ = p.params.OnFailed(err)
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
