// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package transferleader

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// fsm state change: Begin -> UpdateMetadata -> CloseOldLeader -> OpenNewLeader -> Finish
// TODO: add more detailed comments.
const (
	eventUpdateMetadata = "EventUpdateMetadata"
	eventCloseOldLeader = "EventCloseOldLeader"
	eventOpenNewLeader  = "EventOpenNewLeader"
	eventFinish         = "EventFinish"

	stateBegin          = "StateBegin"
	stateUpdateMetadata = "StateUpdateMetadata"
	stateCloseOldLeader = "StateCloseOldLeader"
	stateOpenNewLeader  = "StateOpenNewLeader"
	stateFinish         = "StateFinish"
)

var (
	transferLeaderEvents = fsm.Events{
		{Name: eventUpdateMetadata, Src: []string{stateBegin}, Dst: stateUpdateMetadata},
		{Name: eventCloseOldLeader, Src: []string{stateUpdateMetadata}, Dst: stateCloseOldLeader},
		{Name: eventOpenNewLeader, Src: []string{stateCloseOldLeader}, Dst: stateOpenNewLeader},
		{Name: eventFinish, Src: []string{stateOpenNewLeader}, Dst: stateFinish},
	}
	transferLeaderCallbacks = fsm.Callbacks{
		eventUpdateMetadata: updateMetadataCallback,
		eventCloseOldLeader: closeOldLeaderCallback,
		eventOpenNewLeader:  openNewShardCallback,
		eventFinish:         finishCallback,
	}
)

type Procedure struct {
	id  uint64
	fsm *fsm.FSM

	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch
	storage  procedure.Storage

	shardID           storage.ShardID
	oldLeaderNodeName string
	newLeaderNodeName string

	// Protect the state.
	lock  sync.RWMutex
	state procedure.State
}

// rawData used for storage, procedure will be converted to persist raw data before saved in storage.
type rawData struct {
	ID       uint64
	FsmState string
	State    procedure.State

	ShardID           storage.ShardID
	OldLeaderNodeName string
	NewLeaderNodeName string
}

// callbackRequest is fsm callbacks param.
type callbackRequest struct {
	cluster  *cluster.Cluster
	ctx      context.Context
	dispatch eventdispatch.Dispatch

	shardID           storage.ShardID
	oldLeaderNodeName string
	newLeaderNodeName string
}

func NewProcedure(dispatch eventdispatch.Dispatch, c *cluster.Cluster, s procedure.Storage, shardID storage.ShardID, oldLeaderNodeName string, newLeaderNodeName string, id uint64) (procedure.Procedure, error) {
	shardNodes, err := c.GetShardNodesByShardID(shardID)
	if err != nil {
		log.Error("get shard failed", zap.Error(err))
		return nil, cluster.ErrShardNotFound
	}
	if len(shardNodes) == 0 {
		log.Error("shard not exist in any node", zap.Uint32("shardID", uint32(shardID)))
		return nil, cluster.ErrNodeNotFound
	}
	for _, shardNode := range shardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			leaderNodeName := shardNode.NodeName
			if leaderNodeName != oldLeaderNodeName {
				log.Error("shard leader node not match", zap.String("requestOldLeaderNodeName", oldLeaderNodeName), zap.String("actualOldLeaderNodeName", leaderNodeName))
				return nil, cluster.ErrNodeNotFound
			}
		}
	}
	transferLeaderOperationFsm := fsm.NewFSM(
		stateBegin,
		transferLeaderEvents,
		transferLeaderCallbacks,
	)

	return &Procedure{
		id:                id,
		fsm:               transferLeaderOperationFsm,
		dispatch:          dispatch,
		cluster:           c,
		storage:           s,
		shardID:           shardID,
		oldLeaderNodeName: oldLeaderNodeName,
		newLeaderNodeName: newLeaderNodeName,
		state:             procedure.StateInit,
	}, nil
}

func (p *Procedure) ID() uint64 {
	return p.id
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.TransferLeader
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	transferLeaderRequest := callbackRequest{
		cluster:           p.cluster,
		ctx:               ctx,
		dispatch:          p.dispatch,
		shardID:           p.shardID,
		oldLeaderNodeName: p.oldLeaderNodeName,
		newLeaderNodeName: p.newLeaderNodeName,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			if err := p.fsm.Event(eventUpdateMetadata, transferLeaderRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "transferLeader procedure update metadata")
			}
		case stateUpdateMetadata:
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
	err = p.storage.CreateOrUpdate(ctx, meta)
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

func updateMetadataCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := req.ctx

	if req.cluster.GetClusterState() != storage.ClusterStateStable {
		procedure.CancelEventWithLog(event, cluster.ErrClusterStateInvalid, "cluster state must be stable", zap.Int("currentState", int(req.cluster.GetClusterState())))
		return
	}

	getNodeShardResult, err := req.cluster.GetNodeShards(ctx)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get shardNodes by shardID failed")
		return
	}

	found := false
	shardNodes := make([]storage.ShardNode, 0, len(getNodeShardResult.NodeShards))
	var leaderShardNode storage.ShardNode
	for _, shardNodeWithVersion := range getNodeShardResult.NodeShards {
		if shardNodeWithVersion.ShardNode.ShardRole == storage.ShardRoleLeader {
			leaderShardNode = shardNodeWithVersion.ShardNode
			if leaderShardNode.ID == req.shardID {
				found = true
				leaderShardNode.NodeName = req.newLeaderNodeName
			}
			shardNodes = append(shardNodes, leaderShardNode)
		}
	}
	if !found {
		procedure.CancelEventWithLog(event, procedure.ErrShardLeaderNotFound, "shard leader not found", zap.Uint32("shardID", uint32(req.shardID)))
		return
	}

	err = req.cluster.UpdateClusterView(ctx, storage.ClusterStateStable, shardNodes)
	if err != nil {
		procedure.CancelEventWithLog(event, storage.ErrUpdateClusterViewConflict, "update cluster view")
		return
	}
}

func closeOldLeaderCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := request.ctx

	closeShardRequest := eventdispatch.CloseShardRequest{
		ShardID: uint32(request.shardID),
	}
	if err := request.dispatch.CloseShard(ctx, request.oldLeaderNodeName, closeShardRequest); err != nil {
		procedure.CancelEventWithLog(event, err, "close shard", zap.Uint32("shardID", uint32(request.shardID)), zap.String("oldLeaderName", request.oldLeaderNodeName))
		return
	}
}

func openNewShardCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := request.ctx

	getNodeShardResult, err := request.cluster.GetNodeShards(ctx)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get node shards")
		return
	}
	var preVersion uint64
	for _, shardNodeWithVersion := range getNodeShardResult.NodeShards {
		if request.shardID == shardNodeWithVersion.ShardNode.ID {
			preVersion = shardNodeWithVersion.ShardInfo.Version
		}
	}

	openShardRequest := eventdispatch.OpenShardRequest{
		Shard: cluster.ShardInfo{ID: request.shardID, Role: storage.ShardRoleLeader, Version: preVersion + 1},
	}

	if err := request.dispatch.OpenShard(ctx, request.newLeaderNodeName, openShardRequest); err != nil {
		procedure.CancelEventWithLog(event, err, "open shard", zap.Uint32("shardID", uint32(request.shardID)), zap.String("newLeaderNode", request.newLeaderNodeName))
		return
	}
}

func finishCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	log.Info("transfer leader finish", zap.Uint32("shardID", uint32(request.shardID)), zap.String("oldLeaderNode", request.oldLeaderNodeName), zap.String("newLeaderNode", request.newLeaderNodeName))
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
		ID:                p.id,
		FsmState:          p.fsm.Current(),
		ShardID:           p.shardID,
		OldLeaderNodeName: p.oldLeaderNodeName,
		NewLeaderNodeName: p.newLeaderNodeName,
		State:             p.state,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return procedure.Meta{}, procedure.ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%v, err:%v", p.shardID, err)
	}

	meta := procedure.Meta{
		ID:    p.id,
		Typ:   procedure.TransferLeader,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
