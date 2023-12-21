/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package createtable

import (
	"context"
	"sync"

	"github.com/apache/incubator-horaedb-meta/pkg/assert"
	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/eventdispatch"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/ddl"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/apache/incubator-horaedb-proto/golang/pkg/metaservicepb"
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
		ShardID:       params.ShardID,
		LatestVersion: req.p.relatedVersionInfo.ShardWithVersion[params.ShardID],
	}

	createTableRequest := ddl.BuildCreateTableRequest(result.Table, shardVersionUpdate, params.SourceReq)
	latestShardVersion, err := ddl.CreateTableOnShard(req.ctx, params.ClusterMetadata, params.Dispatch, params.ShardID, createTableRequest)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "dispatch create table on shard")
		return
	}

	log.Debug("dispatch createTableOnShard finish", zap.String("tableName", createTableMetadataRequest.TableName))

	shardVersionUpdate.LatestVersion = latestShardVersion
	err = params.ClusterMetadata.AddTableTopology(req.ctx, shardVersionUpdate, result.Table)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "add table topology")
		return
	}

	log.Debug("add table topology finish", zap.String("tableName", createTableMetadataRequest.TableName))

	req.createTableResult = &metadata.CreateTableResult{
		Table:              result.Table,
		ShardVersionUpdate: shardVersionUpdate,
	}
}

func successCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	assert.Assert(req.createTableResult != nil)
	if err := req.p.params.OnSucceeded(*req.createTableResult); err != nil {
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
	ctx context.Context
	p   *Procedure

	createTableResult *metadata.CreateTableResult
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
		lock:               sync.RWMutex{},
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

func (p *Procedure) Kind() procedure.Kind {
	return procedure.CreateTable
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateState(procedure.StateRunning)

	req := &callbackRequest{
		ctx:               ctx,
		p:                 p,
		createTableResult: nil,
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
