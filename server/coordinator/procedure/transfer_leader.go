// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// fsm state change: Begin -> UpdateMetadata -> CloseOldLeader -> OpenNewLeader -> Finish
// TODO: add more detailed comments.
const (
	eventTransferLeaderUpdateMetadata = "EventTransferLeaderUpdateMetadata"
	eventTransferLeaderCloseOldLeader = "EventTransferLeaderCloseOldLeader"
	eventTransferLeaderOpenNewLeader  = "EventTransferLeaderOpenNewLeader"
	eventTransferLeaderFinish         = "EventTransferLeaderFinish"

	stateTransferLeaderBegin          = "StateTransferLeaderBegin"
	stateTransferLeaderUpdateMetadata = "StateTransferLeaderUpdateMetadata"
	stateTransferLeaderCloseOldLeader = "StateTransferLeaderCloseOldLeader"
	stateTransferLeaderOpenNewLeader  = "StateTransferLeaderOpenNewLeader"
	stateTransferLeaderFinish         = "StateTransferLeaderFinish"
)

var (
	transferLeaderEvents = fsm.Events{
		{Name: eventTransferLeaderUpdateMetadata, Src: []string{stateTransferLeaderBegin}, Dst: stateTransferLeaderUpdateMetadata},
		{Name: eventTransferLeaderCloseOldLeader, Src: []string{stateTransferLeaderUpdateMetadata}, Dst: stateTransferLeaderCloseOldLeader},
		{Name: eventTransferLeaderOpenNewLeader, Src: []string{stateTransferLeaderCloseOldLeader}, Dst: stateTransferLeaderOpenNewLeader},
		{Name: eventTransferLeaderFinish, Src: []string{stateTransferLeaderOpenNewLeader}, Dst: stateTransferLeaderFinish},
	}
	transferLeaderCallbacks = fsm.Callbacks{
		eventTransferLeaderUpdateMetadata: transferLeaderUpdateMetadataCallback,
		eventTransferLeaderCloseOldLeader: transferLeaderCloseOldLeaderCallback,
		eventTransferLeaderOpenNewLeader:  transferLeaderOpenNewShardCallback,
		eventTransferLeaderFinish:         transferLeaderFinishCallback,
	}
)

type TransferLeaderProcedure struct {
	id  uint64
	fsm *fsm.FSM

	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch
	storage  Storage

	shardID           storage.ShardID
	oldLeaderNodeName string
	newLeaderNodeName string

	// Protect the state.
	lock  sync.RWMutex
	state State
}

// TransferLeaderProcedurePersistRawData used for storage, procedure will be converted to persist raw data before saved in storage.
type TransferLeaderProcedurePersistRawData struct {
	ID                uint64
	FsmState          string
	ShardID           storage.ShardID
	OldLeaderNodeName string
	NewLeaderNodeName string
	State             State
}

// TransferLeaderCallbackRequest is fsm callbacks param.
type TransferLeaderCallbackRequest struct {
	cluster  *cluster.Cluster
	ctx      context.Context
	dispatch eventdispatch.Dispatch

	shardID           storage.ShardID
	oldLeaderNodeName string
	newLeaderNodeName string
}

func NewTransferLeaderProcedure(dispatch eventdispatch.Dispatch, c *cluster.Cluster, s Storage, shardID storage.ShardID, oldLeaderNodeName string, newLeaderNodeName string, id uint64) (Procedure, error) {
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
		stateTransferLeaderBegin,
		transferLeaderEvents,
		transferLeaderCallbacks,
	)

	return &TransferLeaderProcedure{id: id, fsm: transferLeaderOperationFsm, dispatch: dispatch, cluster: c, storage: s, shardID: shardID, oldLeaderNodeName: oldLeaderNodeName, newLeaderNodeName: newLeaderNodeName, state: StateInit}, nil
}

func (p *TransferLeaderProcedure) ID() uint64 {
	return p.id
}

func (p *TransferLeaderProcedure) Typ() Typ {
	return TransferLeader
}

func (p *TransferLeaderProcedure) Start(ctx context.Context) error {
	p.updateStateWithLock(StateRunning)

	transferLeaderRequest := TransferLeaderCallbackRequest{
		cluster:           p.cluster,
		ctx:               ctx,
		dispatch:          p.dispatch,
		shardID:           p.shardID,
		oldLeaderNodeName: p.oldLeaderNodeName,
		newLeaderNodeName: p.newLeaderNodeName,
	}

	for {
		switch p.fsm.Current() {
		case stateTransferLeaderBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			if err := p.fsm.Event(eventTransferLeaderUpdateMetadata, transferLeaderRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "trasnferLeader procedure update metadata")
			}
		case stateTransferLeaderUpdateMetadata:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			if err := p.fsm.Event(eventTransferLeaderCloseOldLeader, transferLeaderRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "trasnferLeader procedure close old leader")
			}
		case stateTransferLeaderCloseOldLeader:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			if err := p.fsm.Event(eventTransferLeaderOpenNewLeader, transferLeaderRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "trasnferLeader procedure oepn new leader")
			}
		case stateTransferLeaderOpenNewLeader:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			if err := p.fsm.Event(eventTransferLeaderFinish, transferLeaderRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "trasnferLeader procedure finish")
			}
		case stateTransferLeaderFinish:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			p.updateStateWithLock(StateFinished)
			return nil
		}
	}
}

func (p *TransferLeaderProcedure) persist(ctx context.Context) error {
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

func (p *TransferLeaderProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(StateCancelled)
	return nil
}

func (p *TransferLeaderProcedure) State() State {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.state
}

func transferLeaderUpdateMetadataCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[TransferLeaderCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := request.ctx

	if request.cluster.GetClusterState() != storage.ClusterStateStable {
		cancelEventWithLog(event, cluster.ErrClusterStateInvalid, "cluster state must be stable", zap.Int("currentState", int(request.cluster.GetClusterState())))
		return
	}

	shardNodes, err := request.cluster.GetShardNodesByShardID(request.shardID)
	if err != nil {
		cancelEventWithLog(event, err, "get shardNodes by shardID failed")
		return
	}

	found := false
	var leaderShardNode storage.ShardNode
	for _, shardNode := range shardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			found = true
			leaderShardNode = shardNode
			leaderShardNode.NodeName = request.newLeaderNodeName
		}
	}
	if !found {
		cancelEventWithLog(event, ErrShardLeaderNotFound, "shard leader not found", zap.Uint32("shardID", uint32(request.shardID)))
		return
	}

	err = request.cluster.UpdateClusterView(ctx, storage.ClusterStateStable, []storage.ShardNode{leaderShardNode})
	if err != nil {
		cancelEventWithLog(event, storage.ErrUpdateClusterViewConflict, "update cluster view")
		return
	}
}

func transferLeaderCloseOldLeaderCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[TransferLeaderCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := request.ctx

	closeShardRequest := eventdispatch.CloseShardRequest{
		ShardID: uint32(request.shardID),
	}
	if err := request.dispatch.CloseShard(ctx, request.oldLeaderNodeName, closeShardRequest); err != nil {
		cancelEventWithLog(event, err, "close shard", zap.Uint32("shardID", uint32(request.shardID)), zap.String("oldLeaderName", request.oldLeaderNodeName))
		return
	}
}

func transferLeaderOpenNewShardCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[TransferLeaderCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := request.ctx

	getNodeShardResult, err := request.cluster.GetNodeShards(ctx)
	if err != nil {
		cancelEventWithLog(event, err, "get node shards")
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
		cancelEventWithLog(event, err, "open shard", zap.Uint32("shardID", uint32(request.shardID)), zap.String("newLeaderNode", request.newLeaderNodeName))
		return
	}
}

func transferLeaderFinishCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[TransferLeaderCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	log.Info("transfer leader finish", zap.Uint32("shardID", uint32(request.shardID)), zap.String("oldLeaderNode", request.oldLeaderNodeName), zap.String("newLeaderNode", request.newLeaderNodeName))
}

func (p *TransferLeaderProcedure) updateStateWithLock(state State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}

// TODO: Consider refactor meta procedure convertor function, encapsulate as a tool function.
func (p *TransferLeaderProcedure) convertToMeta() (Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := TransferLeaderProcedurePersistRawData{
		ID:                p.id,
		FsmState:          p.fsm.Current(),
		ShardID:           p.shardID,
		OldLeaderNodeName: p.oldLeaderNodeName,
		NewLeaderNodeName: p.newLeaderNodeName,
		State:             p.state,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return Meta{}, ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%v, err:%v", p.shardID, err)
	}

	meta := Meta{
		ID:    p.id,
		Typ:   TransferLeader,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
