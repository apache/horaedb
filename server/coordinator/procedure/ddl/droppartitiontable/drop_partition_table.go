// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package droppartitiontable

import (
	"context"
	"encoding/json"
	"fmt"
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

// fsm state change:
// ┌────────┐     ┌────────────────┐     ┌────────────────────┐      ┌───────────┐
// │ Begin  ├─────▶  DropDataTable ├─────▶ DropPartitionTable ├──────▶  Finish   │
// └────────┘     └────────────────┘     └────────────────────┘      └───────────┘
const (
	eventDropDataTable      = "EventDropDataTable"
	eventDropPartitionTable = "EventDropPartitionTable"
	eventFinish             = "EventFinish"

	stateBegin              = "StateBegin"
	stateDropDataTable      = "StateDropDataTable"
	stateDropPartitionTable = "StateDropPartitionTable"
	stateFinish             = "StateFinish"
)

var (
	createDropPartitionTableEvents = fsm.Events{
		{Name: eventDropDataTable, Src: []string{stateBegin}, Dst: stateDropDataTable},
		{Name: eventDropPartitionTable, Src: []string{stateDropDataTable}, Dst: stateDropPartitionTable},
		{Name: eventFinish, Src: []string{stateDropPartitionTable}, Dst: stateFinish},
	}
	createDropPartitionTableCallbacks = fsm.Callbacks{
		eventDropDataTable:      dropDataTablesCallback,
		eventDropPartitionTable: dropPartitionTableCallback,
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
	ID              uint64
	ClusterMetadata *metadata.ClusterMetadata
	ClusterSnapshot metadata.Snapshot
	Dispatch        eventdispatch.Dispatch
	Storage         procedure.Storage
	SourceReq       *metaservicepb.DropTableRequest
	OnSucceeded     func(result metadata.TableInfo) error
	OnFailed        func(error) error
}

func NewProcedure(params ProcedureParams) (*Procedure, bool, error) {
	fsm := fsm.NewFSM(
		stateBegin,
		createDropPartitionTableEvents,
		createDropPartitionTableCallbacks,
	)
	relatedVersionInfo, err := buildRelatedVersionInfo(params)
	if err != nil {
		return nil, false, err
	}

	return &Procedure{
		fsm:                fsm,
		params:             params,
		relatedVersionInfo: relatedVersionInfo,
	}, true, nil
}

func buildRelatedVersionInfo(params ProcedureParams) (procedure.RelatedVersionInfo, error) {
	tableShardMapping := make(map[storage.TableID]storage.ShardID, len(params.SourceReq.PartitionTableInfo.GetSubTableNames()))
	for shardID, shardView := range params.ClusterSnapshot.Topology.ShardViewsMapping {
		for _, tableID := range shardView.TableIDs {
			tableShardMapping[tableID] = shardID
		}
	}
	shardViewWithVersion := make(map[storage.ShardID]uint64, 0)
	for _, subTableName := range params.SourceReq.PartitionTableInfo.GetSubTableNames() {
		table, exists, err := params.ClusterMetadata.GetTable(params.SourceReq.GetSchemaName(), subTableName)
		if err != nil {
			return procedure.RelatedVersionInfo{}, errors.WithMessagef(err, "get sub table, tableName:%s", subTableName)
		}
		if !exists {
			return procedure.RelatedVersionInfo{}, errors.WithMessagef(procedure.ErrTableNotExists, "get sub table, tableName:%s", subTableName)
		}
		shardID, exists := tableShardMapping[table.ID]
		if !exists {
			return procedure.RelatedVersionInfo{}, errors.WithMessagef(metadata.ErrShardNotFound, "get shard of sub table, tableID:%d", table.ID)
		}
		shardView, exists := params.ClusterSnapshot.Topology.ShardViewsMapping[shardID]
		if !exists {
			return procedure.RelatedVersionInfo{}, errors.WithMessagef(metadata.ErrShardNotFound, "shard not found in topology, shardID:%d", shardID)
		}
		shardViewWithVersion[shardID] = shardView.Version
	}

	relatedVersionInfo := procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapshot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardViewWithVersion,
		ClusterVersion:   params.ClusterSnapshot.Topology.ClusterView.Version,
	}
	return relatedVersionInfo, nil
}

func (p *Procedure) ID() uint64 {
	return p.params.ID
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.DropPartitionTable
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityMed
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	dropPartitionTableRequest := &callbackRequest{
		ctx: ctx,
		p:   p,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventDropDataTable, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "drop partition table procedure")
			}
		case stateDropDataTable:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventDropPartitionTable, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "drop partition table procedure drop data table")
			}
		case stateDropPartitionTable:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventFinish, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "drop partition table procedure drop partition table")
			}
		case stateFinish:
			p.updateStateWithLock(procedure.StateFinished)
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
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

func (p *Procedure) convertToMeta() (procedure.Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := rawData{
		ID:               p.params.ID,
		FsmState:         p.fsm.Current(),
		State:            p.state,
		DropTableRequest: p.params.SourceReq,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return procedure.Meta{}, procedure.ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%d, err:%v", p.params.ID, err)
	}

	meta := procedure.Meta{
		ID:    p.params.ID,
		Typ:   procedure.DropPartitionTable,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}

type rawData struct {
	ID       uint64
	FsmState string
	State    procedure.State

	DropTableRequest *metaservicepb.DropTableRequest
}

type callbackRequest struct {
	ctx context.Context
	p   *Procedure

	table storage.Table
}

func (d *callbackRequest) schemaName() string {
	return d.p.params.SourceReq.GetSchemaName()
}

func (d *callbackRequest) tableName() string {
	return d.p.params.SourceReq.GetName()
}

// 1. Drop data tables in target nodes.
func dropDataTablesCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	params := req.p.params

	if len(params.SourceReq.PartitionTableInfo.SubTableNames) == 0 {
		procedure.CancelEventWithLog(event, procedure.ErrEmptyPartitionNames, fmt.Sprintf("drop table, table:%s", params.SourceReq.Name))
		return
	}

	shardVersions := req.p.relatedVersionInfo.ShardWithVersion
	for _, tableName := range params.SourceReq.PartitionTableInfo.SubTableNames {
		table, shardVersionUpdate, err := ddl.GetShardVersionByTableName(params.ClusterMetadata, req.schemaName(), tableName, shardVersions)
		if err != nil {
			procedure.CancelEventWithLog(event, err, fmt.Sprintf("get shard version by table, table:%s", tableName))
			return
		}

		if err := ddl.DispatchDropTable(req.ctx, params.ClusterMetadata, params.Dispatch, params.SourceReq.GetSchemaName(), table, shardVersionUpdate); err != nil {
			procedure.CancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", tableName))
			return
		}

		_, err = params.ClusterMetadata.DropTable(req.ctx, req.schemaName(), tableName)
		if err != nil {
			procedure.CancelEventWithLog(event, err, "drop table metadata", zap.String("tableName", tableName))
			return
		}

		shardVersions[shardVersionUpdate.ShardID]++
	}
}

// 2. Drop partition table in target node.
func dropPartitionTableCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	dropTableMetadataResult, err := req.p.params.ClusterMetadata.DropTableMetadata(req.ctx, req.schemaName(), req.tableName())
	if err != nil {
		procedure.CancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", req.tableName()))
		return
	}

	req.table = dropTableMetadataResult.Table
}

func finishCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	log.Info("drop partition table finish")

	tableInfo := metadata.TableInfo{
		ID:         request.table.ID,
		Name:       request.table.Name,
		SchemaID:   request.table.SchemaID,
		SchemaName: request.p.params.SourceReq.GetSchemaName(),
	}

	if err = request.p.params.OnSucceeded(tableInfo); err != nil {
		procedure.CancelEventWithLog(event, err, "drop partition table on succeeded")
		return
	}
}
