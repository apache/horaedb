// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package split

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Fsm state change: begin -> CreateNewShardView -> UpdateShardTables -> OpenNewShard -> Finish
// CreateNewShardView will create new shard metadata.
// UpdateShardTables will update shard tables mapping between the old and new shard.
// OpenNewShard will send open shard request to new shard leader.
const (
	eventCreateNewShardView = "EventCreateNewShardView"
	eventUpdateShardTables  = "EventUpdateShardTables"
	eventOpenNewShard       = "EventOpenNewShard"
	eventFinish             = "EventFinish"

	stateBegin              = "StateBegin"
	stateCreateNewShardView = "StateCreateNewShardView"
	stateUpdateShardTables  = "StateUpdateShardTables"
	stateOpenNewShard       = "StateOpenNewShard"
	stateFinish             = "StateFinish"
)

var (
	splitEvents = fsm.Events{
		{Name: eventCreateNewShardView, Src: []string{stateBegin}, Dst: stateCreateNewShardView},
		{Name: eventUpdateShardTables, Src: []string{stateCreateNewShardView}, Dst: stateUpdateShardTables},
		{Name: eventOpenNewShard, Src: []string{stateUpdateShardTables}, Dst: stateOpenNewShard},
		{Name: eventFinish, Src: []string{stateOpenNewShard}, Dst: stateFinish},
	}
	splitCallbacks = fsm.Callbacks{
		eventCreateNewShardView: createShardViewCallback,
		eventUpdateShardTables:  updateShardTablesCallback,
		eventOpenNewShard:       openShardCallback,
		eventFinish:             finishCallback,
	}
)

type Procedure struct {
	fsm                *fsm.FSM
	params             ProcedureParams
	relatedVersionInfo procedure.RelatedVersionInfo

	// Protect the state.
	lock  sync.RWMutex
	state procedure.State
}

type ProcedureParams struct {
	Id uint64

	Dispatch eventdispatch.Dispatch
	Storage  procedure.Storage

	ClusterMetadata *metadata.ClusterMetadata
	ClusterSnapShot metadata.Snapshot

	ShardID    storage.ShardID
	NewShardID storage.ShardID

	SchemaName     string
	TableNames     []string
	TargetNodeName string
}

func NewProcedure(params ProcedureParams) (procedure.Procedure, error) {
	if err := validateClusterTopology(params.ClusterSnapShot.Topology, params.ShardID); err != nil {
		return nil, err
	}

	relatedVersionInfo := buildRelatedVersionInfo(params)

	splitFsm := fsm.NewFSM(
		stateBegin,
		splitEvents,
		splitCallbacks,
	)

	return &Procedure{
		fsm:                splitFsm,
		relatedVersionInfo: relatedVersionInfo,
		params:             params,
		state:              procedure.StateInit,
	}, nil
}

func buildRelatedVersionInfo(params ProcedureParams) procedure.RelatedVersionInfo {
	shardWithVersion := make(map[storage.ShardID]uint64, 0)
	for _, shardView := range params.ClusterSnapShot.Topology.ShardViews {
		if shardView.ShardID == params.ShardID {
			shardWithVersion[params.ShardID] = shardView.Version
		}
	}
	shardWithVersion[params.NewShardID] = 0
	relatedVersionInfo := procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapShot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardWithVersion,
		ClusterVersion:   params.ClusterSnapShot.Topology.ClusterView.Version,
	}
	return relatedVersionInfo
}

func validateClusterTopology(topology metadata.Topology, shardID storage.ShardID) error {
	// Validate cluster state.
	curState := topology.ClusterView.State
	if curState != storage.ClusterStateStable {
		log.Error("cluster state must be stable", zap.Error(metadata.ErrClusterStateInvalid))
		return metadata.ErrClusterStateInvalid
	}

	found := false
	for _, shardView := range topology.ShardViews {
		if shardView.ShardID == shardID {
			found = true
			break
		}
	}
	if !found {
		log.Error("shard not found", zap.Uint64("shardID", uint64(shardID)), zap.Error(metadata.ErrShardNotFound))
		return metadata.ErrShardNotFound
	}

	found = false
	for _, shardNode := range topology.ClusterView.ShardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			found = true
		}
	}
	if !found {
		log.Error("shard leader not found", zap.Error(procedure.ErrShardLeaderNotFound))
		return procedure.ErrShardLeaderNotFound
	}
	return nil
}

type callbackRequest struct {
	ctx context.Context
	p   *Procedure
}

func (p *Procedure) ID() uint64 {
	return p.params.Id
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.Split
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityHigh
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	splitCallbackRequest := callbackRequest{
		ctx: ctx,
		p:   p,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventCreateNewShardView, splitCallbackRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "split procedure create new shard view")
			}
		case stateCreateNewShardView:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventUpdateShardTables, splitCallbackRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "split procedure create new shard")
			}
		case stateUpdateShardTables:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventOpenNewShard, splitCallbackRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "split procedure create shard tables")
			}
		case stateOpenNewShard:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventFinish, splitCallbackRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "split procedure delete shard tables")
			}
		case stateFinish:
			// TODO: The state update sequence here is inconsistent with the previous one. Consider reconstructing the state update logic of the state machine.
			p.updateStateWithLock(procedure.StateFinished)
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			return nil
		}
	}
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

func (p *Procedure) updateStateWithLock(state procedure.State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}

func createShardViewCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	if err := req.p.params.ClusterMetadata.CreateShardViews(req.ctx, []metadata.CreateShardView{{
		ShardID: req.p.params.ShardID,
		Tables:  []storage.TableID{},
	}}); err != nil {
		procedure.CancelEventWithLog(event, err, "create shard views")
		return
	}
}

func updateShardTablesCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	if err := request.p.params.ClusterMetadata.MigrateTable(request.ctx, metadata.MigrateTableRequest{
		SchemaName: request.p.params.SchemaName,
		TableNames: request.p.params.TableNames,
		OldShardID: request.p.params.ShardID,
		NewShardID: request.p.params.NewShardID,
	}); err != nil {
		procedure.CancelEventWithLog(event, err, "update shard tables")
		return
	}
}

func openShardCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := request.ctx

	// Send open new shard request to CSE.
	if err := request.p.params.Dispatch.OpenShard(ctx, request.p.params.TargetNodeName, eventdispatch.OpenShardRequest{
		Shard: metadata.ShardInfo{
			ID:      request.p.params.NewShardID,
			Role:    storage.ShardRoleLeader,
			Version: 0,
		},
	}); err != nil {
		procedure.CancelEventWithLog(event, err, "open shard failed")
		return
	}
}

func finishCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	log.Info("split procedure finish", zap.Uint32("shardID", uint32(request.p.params.ShardID)), zap.Uint32("newShardID", uint32(request.p.params.NewShardID)))
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

type rawData struct {
	SchemaName     string
	TableNames     []string
	ShardID        uint32
	NewShardID     uint32
	TargetNodeName string
}

func (p *Procedure) convertToMeta() (procedure.Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := rawData{
		SchemaName:     p.params.SchemaName,
		TableNames:     p.params.TableNames,
		ShardID:        uint32(p.params.ShardID),
		NewShardID:     uint32(p.params.NewShardID),
		TargetNodeName: p.params.TargetNodeName,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return procedure.Meta{}, procedure.ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%v, err:%v", p.params.ShardID, err)
	}

	meta := procedure.Meta{
		ID:    p.params.Id,
		Typ:   procedure.Split,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
