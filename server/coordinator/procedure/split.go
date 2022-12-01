// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	eventSplitCreateNewShardMetadata = "EventSplitCreateNewShardMetadata"
	eventSplitCreateNewShardView     = "EventCreateNewShardView"
	eventSplitUpdateShardTables      = "EventSplitUpdateShardTables"
	eventSplitOpenNewShard           = "EventSplitOpenNewShard"
	eventSplitFinish                 = "EventSplitFinish"

	stateSplitBegin                  = "StateBegin"
	stateSplitCreateNewShardMetadata = "StateSplitCreateNewShardMetadata"
	stateSplitCreateNewShardView     = "StateSplitCreateNewShardView"
	stateSplitUpdateShardTables      = "StateSplitUpdateShardTables"
	stateSplitOpenNewShard           = "StateOpenNewShard"
	stateSplitFinish                 = "StateFinish"
)

var (
	splitEvents = fsm.Events{
		{Name: eventSplitCreateNewShardMetadata, Src: []string{stateSplitBegin}, Dst: stateSplitCreateNewShardMetadata},
		{Name: eventSplitCreateNewShardView, Src: []string{stateSplitCreateNewShardMetadata}, Dst: stateSplitCreateNewShardView},
		{Name: eventSplitUpdateShardTables, Src: []string{stateSplitCreateNewShardView}, Dst: stateSplitUpdateShardTables},
		{Name: eventSplitOpenNewShard, Src: []string{stateSplitUpdateShardTables}, Dst: stateSplitOpenNewShard},
		{Name: eventSplitFinish, Src: []string{stateSplitOpenNewShard}, Dst: stateSplitFinish},
	}
	splitCallbacks = fsm.Callbacks{
		eventSplitCreateNewShardMetadata: splitOpenNewShardMetadataCallback,
		eventSplitCreateNewShardView:     splitCreateShardViewCallback,
		eventSplitUpdateShardTables:      splitUpdateShardTablesCallback,
		eventSplitOpenNewShard:           splitOpenShardCallback,
		eventSplitFinish:                 splitFinishCallback,
	}
)

// SplitProcedure fsm: Update ShardTable Metadata -> OpenNewShard -> CloseTable
type SplitProcedure struct {
	id uint64

	fsm *fsm.FSM

	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch
	storage  Storage

	shardID        storage.ShardID
	newShardID     storage.ShardID
	tableNames     []string
	targetNodeName string
	schemaName     string

	// Protect the state.
	lock  sync.RWMutex
	state State
}

func NewSplitProcedure(id uint64, dispatch eventdispatch.Dispatch, storage Storage, c *cluster.Cluster, schemaName string, shardID storage.ShardID, newShardID storage.ShardID, tableNames []string, targetNodeName string) *SplitProcedure {
	splitFsm := fsm.NewFSM(
		stateSplitBegin,
		splitEvents,
		splitCallbacks,
	)

	return &SplitProcedure{
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

type splitCallbackRequest struct {
	ctx context.Context

	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	shardID        storage.ShardID
	newShardID     storage.ShardID
	schemaName     string
	tableNames     []string
	targetNodeName string
}

func (p *SplitProcedure) ID() uint64 {
	return p.id
}

func (p *SplitProcedure) Typ() Typ {
	return Split
}

func (p *SplitProcedure) Start(ctx context.Context) error {
	p.updateStateWithLock(StateRunning)

	splitCallbackRequest := splitCallbackRequest{
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
		case stateSplitBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventSplitCreateNewShardMetadata, splitCallbackRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "split procedure create new shard metadata")
			}
		case stateSplitCreateNewShardMetadata:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventSplitCreateNewShardView, splitCallbackRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "split procedure create new shard view")
			}
		case stateSplitCreateNewShardView:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventSplitUpdateShardTables, splitCallbackRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "split procedure create new shard")
			}
		case stateSplitUpdateShardTables:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventSplitOpenNewShard, splitCallbackRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "split procedure create shard tables")
			}
		case stateSplitOpenNewShard:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventSplitFinish, splitCallbackRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "split procedure delete shard tables")
			}
		case stateSplitFinish:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			p.updateStateWithLock(StateFinished)
			return nil
		}
	}
}

func (p *SplitProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(StateCancelled)
	return nil
}

func (p *SplitProcedure) State() State {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.state
}

func (p *SplitProcedure) updateStateWithLock(state State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}

// splitOpenNewShardMetadataCallback create new shard and update metadata, table mapping will be updated in splitCloseTableCallback & splitOpenTableCallback callbacks.
func splitOpenNewShardMetadataCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[splitCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := request.ctx

	// Validate cluster state.
	curState := request.cluster.GetClusterState()
	if curState != storage.ClusterStateStable {
		cancelEventWithLog(event, cluster.ErrClusterStateInvalid, "cluster state must be stable")
		return
	}

	// Validate tables.
	shardTables := request.cluster.GetShardTables([]storage.ShardID{request.shardID}, request.targetNodeName)[request.shardID]
	var tableNames []string
	for _, table := range shardTables.Tables {
		if request.schemaName == table.SchemaName {
			tableNames = append(tableNames, table.Name)
		}
	}

	if !IsSubSlice(request.tableNames, tableNames) {
		cancelEventWithLog(event, cluster.ErrTableNotFound, "split tables not found in shard", zap.String("requestTableNames", strings.Join(request.tableNames, ",")), zap.String("tableNames", strings.Join(tableNames, ",")))
		return
	}

	shardNodes, err := request.cluster.GetShardNodesByShardID(request.shardID)
	if err != nil {
		cancelEventWithLog(event, err, "cluster get shardNode by id")
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
		cancelEventWithLog(event, ErrShardLeaderNotFound, "shard leader not found")
		return
	}

	// Create a new shard on origin node.
	getNodeShardResult, err := request.cluster.GetNodeShards(ctx)
	if err != nil {
		cancelEventWithLog(event, err, "get node shards failed")
		return
	}

	var updateShardNodes []storage.ShardNode
	for _, shardNodeWithVersion := range getNodeShardResult.NodeShards {
		updateShardNodes = append(updateShardNodes, shardNodeWithVersion.ShardNode)
	}
	updateShardNodes = append(updateShardNodes, storage.ShardNode{
		ID:        request.newShardID,
		ShardRole: storage.ShardRoleLeader,
		NodeName:  leaderShardNode.NodeName,
	})

	// Update cluster view metadata.
	if err = request.cluster.UpdateClusterView(ctx, storage.ClusterStateStable, updateShardNodes); err != nil {
		cancelEventWithLog(event, err, "update cluster view failed")
		return
	}
}

func splitCreateShardViewCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[splitCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := request.ctx

	if err := request.cluster.CreateShardViews(ctx, []cluster.CreateShardView{{
		ShardID: request.newShardID,
		Tables:  []storage.TableID{},
	}}); err != nil {
		cancelEventWithLog(event, err, "create shard views")
		return
	}
}

func splitOpenShardCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[splitCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
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
		cancelEventWithLog(event, err, "open shard failed")
		return
	}
}

func splitUpdateShardTablesCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[splitCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := request.ctx

	originShardTables := request.cluster.GetShardTables([]storage.ShardID{request.shardID}, request.targetNodeName)[request.shardID]

	// Find remaining tables in old shard.
	var remainingTables []cluster.TableInfo

	for _, tableInfo := range originShardTables.Tables {
		found := false
		for _, tableName := range request.tableNames {
			if tableInfo.Name == tableName && tableInfo.SchemaName == request.schemaName {
				found = true
				break
			}
		}
		if !found {
			remainingTables = append(remainingTables, tableInfo)
		}
	}

	// Update shard tables.
	originShardTables.Tables = remainingTables
	originShardTables.Shard.Version++

	getNodeShardsResult, err := request.cluster.GetNodeShards(ctx)
	if err != nil {
		cancelEventWithLog(event, err, "get node shards")
		return
	}

	// Find new shard in metadata.
	var newShardInfo cluster.ShardInfo
	found := false
	for _, shardNodeWithVersion := range getNodeShardsResult.NodeShards {
		if shardNodeWithVersion.ShardInfo.ID == request.newShardID {
			newShardInfo = shardNodeWithVersion.ShardInfo
			found = true
			break
		}
	}
	if !found {
		cancelEventWithLog(event, cluster.ErrShardNotFound, "new shard not found", zap.Uint32("shardID", uint32(request.newShardID)))
		return
	}
	newShardInfo.Version++

	// Find split tables in metadata.
	var tables []cluster.TableInfo
	for _, tableName := range request.tableNames {
		table, exists, err := request.cluster.GetTable(request.schemaName, tableName)
		if err != nil {
			cancelEventWithLog(event, err, "get table", zap.String("schemaName", request.schemaName), zap.String("tableName", tableName))
			return
		}
		if !exists {
			cancelEventWithLog(event, cluster.ErrTableNotFound, "table not found", zap.String("schemaName", request.schemaName), zap.String("tableName", tableName))
			return
		}
		tables = append(tables, cluster.TableInfo{
			ID:         table.ID,
			Name:       table.Name,
			SchemaID:   table.SchemaID,
			SchemaName: request.schemaName,
		})
	}
	newShardTables := cluster.ShardTables{
		Shard:  newShardInfo,
		Tables: tables,
	}

	if err := request.cluster.UpdateShardTables(ctx, []cluster.ShardTables{originShardTables, newShardTables}); err != nil {
		cancelEventWithLog(event, err, "update shard tables")
		return
	}
}

func splitFinishCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[splitCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}
	log.Info("split procedure finish", zap.Uint32("shardID", uint32(request.shardID)), zap.Uint32("newShardID", uint32(request.newShardID)))
}

func (p *SplitProcedure) persist(ctx context.Context) error {
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

type SplitProcedurePersistRawData struct {
	SchemaName     string
	TableNames     []string
	ShardID        uint32
	NewShardID     uint32
	TargetNodeName string
}

func (p *SplitProcedure) convertToMeta() (Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := SplitProcedurePersistRawData{
		SchemaName:     p.schemaName,
		TableNames:     p.tableNames,
		ShardID:        uint32(p.shardID),
		NewShardID:     uint32(p.newShardID),
		TargetNodeName: p.targetNodeName,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return Meta{}, ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%v, err:%v", p.shardID, err)
	}

	meta := Meta{
		ID:    p.id,
		Typ:   Split,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
