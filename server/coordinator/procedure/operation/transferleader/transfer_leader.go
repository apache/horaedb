/*
 * Copyright 2022 The CeresDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package transferleader

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/CeresDB/horaemeta/pkg/log"
	"github.com/CeresDB/horaemeta/server/cluster/metadata"
	"github.com/CeresDB/horaemeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/horaemeta/server/coordinator/procedure"
	"github.com/CeresDB/horaemeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Fsm state change: Begin -> CloseOldLeader -> OpenNewLeader -> Finish.
// CloseOldLeader will send close shard request if the old leader node exists.
// OpenNewLeader will send open shard request to new leader node.
const (
	eventCloseOldLeader = "EventCloseOldLeader"
	eventOpenNewLeader  = "EventOpenNewLeader"
	eventFinish         = "EventFinish"

	stateBegin          = "StateBegin"
	stateCloseOldLeader = "StateCloseOldLeader"
	stateOpenNewLeader  = "StateOpenNewLeader"
	stateFinish         = "StateFinish"
)

var (
	transferLeaderEvents = fsm.Events{
		{Name: eventCloseOldLeader, Src: []string{stateBegin}, Dst: stateCloseOldLeader},
		{Name: eventOpenNewLeader, Src: []string{stateCloseOldLeader}, Dst: stateOpenNewLeader},
		{Name: eventFinish, Src: []string{stateOpenNewLeader}, Dst: stateFinish},
	}
	transferLeaderCallbacks = fsm.Callbacks{
		eventCloseOldLeader: closeOldLeaderCallback,
		eventOpenNewLeader:  openNewShardCallback,
		eventFinish:         finishCallback,
	}
)

type Procedure struct {
	fsm                *fsm.FSM
	params             ProcedureParams
	relatedVersionInfo procedure.RelatedVersionInfo

	// Protect the state.
	// FIXME: the procedure should be executed sequentially, so any need to use a lock to protect it?
	lock  sync.RWMutex
	state procedure.State
}

// rawData used for storage, procedure will be converted to persist raw data before saved in storage.
type rawData struct {
	ID       uint64
	FsmState string
	State    procedure.State

	snapshot          metadata.Snapshot
	ShardID           storage.ShardID
	OldLeaderNodeName string
	NewLeaderNodeName string
}

// callbackRequest is fsm callbacks param.
type callbackRequest struct {
	ctx context.Context
	p   *Procedure
}

type ProcedureParams struct {
	ID uint64

	Dispatch eventdispatch.Dispatch
	Storage  procedure.Storage

	ClusterSnapshot metadata.Snapshot

	ShardID           storage.ShardID
	OldLeaderNodeName string
	NewLeaderNodeName string
}

func NewProcedure(params ProcedureParams) (procedure.Procedure, error) {
	if err := validateClusterTopology(params.ClusterSnapshot.Topology, params.ShardID, params.OldLeaderNodeName); err != nil {
		return nil, err
	}

	relatedVersionInfo, err := buildRelatedVersionInfo(params)
	if err != nil {
		return nil, err
	}

	transferLeaderOperationFsm := fsm.NewFSM(
		stateBegin,
		transferLeaderEvents,
		transferLeaderCallbacks,
	)

	return &Procedure{
		fsm:                transferLeaderOperationFsm,
		params:             params,
		relatedVersionInfo: relatedVersionInfo,
		lock:               sync.RWMutex{},
		state:              procedure.StateInit,
	}, nil
}

func buildRelatedVersionInfo(params ProcedureParams) (procedure.RelatedVersionInfo, error) {
	shardViewWithVersion := make(map[storage.ShardID]uint64, 0)
	shardView, exists := params.ClusterSnapshot.Topology.ShardViewsMapping[params.ShardID]
	if !exists {
		return procedure.RelatedVersionInfo{}, errors.WithMessagef(metadata.ErrShardNotFound, "shard not found in topology, shardID:%d", params.ShardID)
	}
	shardViewWithVersion[params.ShardID] = shardView.Version

	relatedVersionInfo := procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapshot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardViewWithVersion,
		ClusterVersion:   params.ClusterSnapshot.Topology.ClusterView.Version,
	}
	return relatedVersionInfo, nil
}

func validateClusterTopology(topology metadata.Topology, shardID storage.ShardID, oldLeaderNodeName string) error {
	_, found := topology.ShardViewsMapping[shardID]
	if !found {
		log.Error("shard not found", zap.Uint64("shardID", uint64(shardID)))
		return metadata.ErrShardNotFound
	}
	if len(oldLeaderNodeName) == 0 {
		return nil
	}
	shardNodes := topology.ClusterView.ShardNodes
	if len(shardNodes) == 0 {
		log.Error("shard not exist in any node", zap.Uint32("shardID", uint32(shardID)))
		return metadata.ErrShardNotFound
	}
	for _, shardNode := range shardNodes {
		if shardNode.ID == shardID {
			leaderNodeName := shardNode.NodeName
			if leaderNodeName != oldLeaderNodeName {
				log.Error("shard leader node not match", zap.String("requestOldLeaderNodeName", oldLeaderNodeName), zap.String("actualOldLeaderNodeName", leaderNodeName))
				return metadata.ErrNodeNotFound
			}
		}
	}
	return nil
}

func (p *Procedure) ID() uint64 {
	return p.params.ID
}

func (p *Procedure) Kind() procedure.Kind {
	return procedure.TransferLeader
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityHigh
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	transferLeaderRequest := callbackRequest{
		ctx: ctx,
		p:   p,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			if err := p.fsm.Event(eventCloseOldLeader, transferLeaderRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "transferLeader procedure close old leader")
			}
		case stateCloseOldLeader:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			if err := p.fsm.Event(eventOpenNewLeader, transferLeaderRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "transferLeader procedure open new leader")
			}
		case stateOpenNewLeader:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			if err := p.fsm.Event(eventFinish, transferLeaderRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "transferLeader procedure finish")
			}
		case stateFinish:
			// TODO: The state update sequence here is inconsistent with the previous one. Consider reconstructing the state update logic of the state machine.
			p.updateStateWithLock(procedure.StateFinished)
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			return nil
		}
	}
}

func (p *Procedure) persist(ctx context.Context) error {
	meta, err := p.convertToMeta()
	if err != nil {
		return errors.WithMessage(err, "convert to meta")
	}
	err = p.params.Storage.CreateOrUpdate(ctx, meta)
	if err != nil {
		return errors.WithMessage(err, "createOrUpdate procedure storage")
	}
	return nil
}

func (p *Procedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(procedure.StateCancelled)
	return nil
}

func (p *Procedure) State() procedure.State {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.state
}

func closeOldLeaderCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := req.ctx

	if len(req.p.params.OldLeaderNodeName) == 0 {
		return
	}

	log.Info("try to close shard", zap.Uint64("procedureID", req.p.ID()), zap.Uint64("shardID", uint64(req.p.params.ShardID)), zap.String("oldLeader", req.p.params.OldLeaderNodeName))

	closeShardRequest := eventdispatch.CloseShardRequest{
		ShardID: uint32(req.p.params.ShardID),
	}
	if err := req.p.params.Dispatch.CloseShard(ctx, req.p.params.OldLeaderNodeName, closeShardRequest); err != nil {
		procedure.CancelEventWithLog(event, err, "close shard", zap.Uint32("shardID", uint32(req.p.params.ShardID)), zap.String("oldLeaderName", req.p.params.OldLeaderNodeName))
		return
	}

	log.Info("close shard finish", zap.Uint64("procedureID", req.p.ID()), zap.Uint64("shardID", req.p.params.ID), zap.String("oldLeader", req.p.params.OldLeaderNodeName))
}

func openNewShardCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := req.ctx

	shardView, exists := req.p.params.ClusterSnapshot.Topology.ShardViewsMapping[req.p.params.ShardID]
	if !exists {
		procedure.CancelEventWithLog(event, metadata.ErrShardNotFound, "shard not found in topology", zap.Uint64("shardID", uint64(req.p.params.ShardID)))
		return
	}

	openShardRequest := eventdispatch.OpenShardRequest{
		Shard: metadata.ShardInfo{
			ID:      req.p.params.ShardID,
			Role:    storage.ShardRoleLeader,
			Version: shardView.Version,
			Status:  storage.ShardStatusUnknown,
		},
	}

	log.Info("try to open shard", zap.Uint64("procedureID", req.p.ID()), zap.Uint64("shardID", uint64(req.p.params.ShardID)), zap.String("newLeader", req.p.params.NewLeaderNodeName))

	if err := req.p.params.Dispatch.OpenShard(ctx, req.p.params.NewLeaderNodeName, openShardRequest); err != nil {
		procedure.CancelEventWithLog(event, err, "open shard", zap.Uint32("shardID", uint32(req.p.params.ShardID)), zap.String("newLeaderNode", req.p.params.NewLeaderNodeName))
		return
	}

	log.Info("open shard finish", zap.Uint64("procedureID", req.p.ID()), zap.Uint64("shardID", uint64(req.p.params.ShardID)), zap.String("newLeader", req.p.params.NewLeaderNodeName))
}

func finishCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	log.Info("transfer leader finish", zap.Uint64("procedureID", req.p.ID()), zap.Uint32("shardID", uint32(req.p.params.ShardID)), zap.String("oldLeaderNode", req.p.params.OldLeaderNodeName), zap.String("newLeaderNode", req.p.params.NewLeaderNodeName))
}

func (p *Procedure) updateStateWithLock(state procedure.State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}

// TODO: Consider refactor meta procedure convertor function, encapsulate as a tool function.
func (p *Procedure) convertToMeta() (procedure.Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := rawData{
		ID:                p.params.ID,
		FsmState:          p.fsm.Current(),
		ShardID:           p.params.ShardID,
		snapshot:          p.params.ClusterSnapshot,
		OldLeaderNodeName: p.params.OldLeaderNodeName,
		NewLeaderNodeName: p.params.NewLeaderNodeName,
		State:             p.state,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		var emptyMeta procedure.Meta
		return emptyMeta, procedure.ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%v, err:%v", p.params.ShardID, err)
	}

	meta := procedure.Meta{
		ID:    p.params.ID,
		Kind:  procedure.TransferLeader,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
