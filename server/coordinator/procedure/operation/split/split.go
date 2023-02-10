// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package split

import (
	"context"
	"encoding/json"
	"strings"
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

const (
	eventCreateNewShardMetadata = "EventCreateNewShardMetadata"
	eventCreateNewShardView     = "EventCreateNewShardView"
	eventUpdateShardTables      = "EventUpdateShardTables"
	eventOpenNewShard           = "EventOpenNewShard"
	eventFinish                 = "EventFinish"

	stateBegin                  = "StateBegin"
	stateCreateNewShardMetadata = "StateCreateNewShardMetadata"
	stateCreateNewShardView     = "StateCreateNewShardView"
	stateUpdateShardTables      = "StateUpdateShardTables"
	stateOpenNewShard           = "StateOpenNewShard"
	stateFinish                 = "StateFinish"
)

var (
	splitEvents = fsm.Events{
		{Name: eventCreateNewShardMetadata, Src: []string{stateBegin}, Dst: stateCreateNewShardMetadata},
		{Name: eventCreateNewShardView, Src: []string{stateCreateNewShardMetadata}, Dst: stateCreateNewShardView},
		{Name: eventUpdateShardTables, Src: []string{stateCreateNewShardView}, Dst: stateUpdateShardTables},
		{Name: eventOpenNewShard, Src: []string{stateUpdateShardTables}, Dst: stateOpenNewShard},
		{Name: eventFinish, Src: []string{stateOpenNewShard}, Dst: stateFinish},
	}
	splitCallbacks = fsm.Callbacks{
		eventCreateNewShardMetadata: openNewShardMetadataCallback,
		eventCreateNewShardView:     createShardViewCallback,
		eventUpdateShardTables:      updateShardTablesCallback,
		eventOpenNewShard:           openShardCallback,
		eventFinish:                 finishCallback,
	}
)

// Procedure fsm: Update ShardTable Metadata -> OpenNewShard -> CloseTable
type Procedure struct {
	id uint64

	fsm *fsm.FSM

	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch
	storage  procedure.Storage

	shardID        storage.ShardID
	newShardID     storage.ShardID
	tableNames     []string
	targetNodeName string
	schemaName     string

	// Protect the state.
	lock  sync.RWMutex
	state procedure.State
}

func NewProcedure(id uint64, dispatch eventdispatch.Dispatch, storage procedure.Storage, c *cluster.Cluster, schemaName string, shardID storage.ShardID, newShardID storage.ShardID, tableNames []string, targetNodeName string) *Procedure {
	splitFsm := fsm.NewFSM(
		stateBegin,
		splitEvents,
		splitCallbacks,
	)

	return &Procedure{
		fsm:            splitFsm,
		id:             id,
		cluster:        c,
		dispatch:       dispatch,
		shardID:        shardID,
		newShardID:     newShardID,
		targetNodeName: targetNodeName,
		tableNames:     tableNames,
		schemaName:     schemaName,
		storage:        storage,
	}
}

type callbackRequest struct {
	ctx context.Context

	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	shardID        storage.ShardID
	newShardID     storage.ShardID
	schemaName     string
	tableNames     []string
	targetNodeName string
}

func (p *Procedure) ID() uint64 {
	return p.id
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.Split
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	splitCallbackRequest := callbackRequest{
		ctx:            ctx,
		cluster:        p.cluster,
		dispatch:       p.dispatch,
		shardID:        p.shardID,
		newShardID:     p.newShardID,
		schemaName:     p.schemaName,
		tableNames:     p.tableNames,
		targetNodeName: p.targetNodeName,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventCreateNewShardMetadata, splitCallbackRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "split procedure create new shard metadata")
			}
		case stateCreateNewShardMetadata:
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

// openNewShardMetadataCallback create new shard and update metadata, table mapping will be updated in splitCloseTableCallback & splitOpenTableCallback callbacks.
func openNewShardMetadataCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := req.ctx

	// Validate cluster state.
	curState := req.cluster.GetClusterState()
	if curState != storage.ClusterStateStable {
		procedure.CancelEventWithLog(event, cluster.ErrClusterStateInvalid, "cluster state must be stable")
		return
	}

	// Validate tables.
	shardTables := req.cluster.GetShardTables([]storage.ShardID{req.shardID}, req.targetNodeName)[req.shardID]
	var tableNames []string
	for _, table := range shardTables.Tables {
		if req.schemaName == table.SchemaName {
			tableNames = append(tableNames, table.Name)
		}
	}

	if !procedure.IsSubSlice(req.tableNames, tableNames) {
		procedure.CancelEventWithLog(event, cluster.ErrTableNotFound, "split tables not found in shard", zap.String("requestTableNames", strings.Join(req.tableNames, ",")), zap.String("tableNames", strings.Join(tableNames, ",")))
		return
	}

	shardNodes, err := req.cluster.GetShardNodesByShardID(req.shardID)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "cluster get shardNode by id")
		return
	}

	var leaderShardNode storage.ShardNode
	found := false
	for _, shardNode := range shardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			leaderShardNode = shardNode
			found = true
		}
	}
	if !found {
		procedure.CancelEventWithLog(event, procedure.ErrShardLeaderNotFound, "shard leader not found")
		return
	}

	// Create a new shard on origin node.
	getNodeShardResult, err := req.cluster.GetNodeShards(ctx)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get node shards failed")
		return
	}

	var updateShardNodes []storage.ShardNode
	for _, shardNodeWithVersion := range getNodeShardResult.NodeShards {
		updateShardNodes = append(updateShardNodes, shardNodeWithVersion.ShardNode)
	}
	updateShardNodes = append(updateShardNodes, storage.ShardNode{
		ID:        req.newShardID,
		ShardRole: storage.ShardRoleLeader,
		NodeName:  leaderShardNode.NodeName,
	})

	// Update cluster view metadata.
	if err = req.cluster.UpdateClusterView(ctx, storage.ClusterStateStable, updateShardNodes); err != nil {
		procedure.CancelEventWithLog(event, err, "update cluster view failed")
		return
	}
}

func createShardViewCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := request.ctx

	if err := request.cluster.CreateShardViews(ctx, []cluster.CreateShardView{{
		ShardID: request.newShardID,
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

	if err := request.cluster.MigrateTable(request.ctx, cluster.MigrateTableRequest{
		SchemaName: request.schemaName,
		TableNames: request.tableNames,
		OldShardID: request.shardID,
		NewShardID: request.newShardID,
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
	if err := request.dispatch.OpenShard(ctx, request.targetNodeName, eventdispatch.OpenShardRequest{
		Shard: cluster.ShardInfo{
			ID:      request.newShardID,
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
	log.Info("split procedure finish", zap.Uint32("shardID", uint32(request.shardID)), zap.Uint32("newShardID", uint32(request.newShardID)))
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
		SchemaName:     p.schemaName,
		TableNames:     p.tableNames,
		ShardID:        uint32(p.shardID),
		NewShardID:     uint32(p.newShardID),
		TargetNodeName: p.targetNodeName,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return procedure.Meta{}, procedure.ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%v, err:%v", p.shardID, err)
	}

	meta := procedure.Meta{
		ID:    p.id,
		Typ:   procedure.Split,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
