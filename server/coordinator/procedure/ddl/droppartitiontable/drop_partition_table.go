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
	fsm    *fsm.FSM
	params ProcedureParams

	// Protect the state.
	lock  sync.RWMutex
	state procedure.State
}

type ProcedureParams struct {
	ID              uint64
	ClusterMetadata *metadata.ClusterMetadata
	Dispatch        eventdispatch.Dispatch
	Storage         procedure.Storage
	SourceReq       *metaservicepb.DropTableRequest
	OnSucceeded     func(result metadata.TableInfo) error
	OnFailed        func(error) error
}

func NewProcedure(params ProcedureParams) *Procedure {
	fsm := fsm.NewFSM(
		stateBegin,
		createDropPartitionTableEvents,
		createDropPartitionTableCallbacks,
	)
	return &Procedure{
		fsm:    fsm,
		params: params,
	}
}

func (p *Procedure) ID() uint64 {
	return p.params.ID
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.DropPartitionTable
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	dropPartitionTableRequest := &callbackRequest{
		ctx: ctx,
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

	for _, tableName := range params.SourceReq.PartitionTableInfo.SubTableNames {
		dropTableResult, err := dropTableMetaData(event, tableName)
		if err != nil {
			procedure.CancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", tableName))
			return
		}

		if !dropTableResult.exists {
			continue
		}

		if len(dropTableResult.result.ShardVersionUpdate) != 1 {
			procedure.CancelEventWithLog(event, procedure.ErrDropTableResult, fmt.Sprintf("legnth of shardVersionResult!=1, current is %d", len(dropTableResult.result.ShardVersionUpdate)))
			return
		}

		if err := dispatchDropTable(event, dropTableResult.table, dropTableResult.result.ShardVersionUpdate[0]); err != nil {
			procedure.CancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", tableName))
			return
		}
	}
}

// 2. Drop partition table in target node.
func dropPartitionTableCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	dropTableResult, err := dropTableMetaData(event, req.tableName())
	if err != nil {
		procedure.CancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", req.tableName()))
		return
	}
	if !dropTableResult.exists {
		procedure.CancelEventWithLog(event, procedure.ErrTableNotExists, fmt.Sprintf("table:%s", req.tableName()))
		return
	}

	if len(dropTableResult.result.ShardVersionUpdate) == 0 {
		procedure.CancelEventWithLog(event, procedure.ErrDropTableResult, fmt.Sprintf("legnth of shardVersionResult need >=1, current is %d", len(dropTableResult.result.ShardVersionUpdate)))
		return
	}

	req.table = dropTableResult.table

	// Drop table in the first shard.
	if err := dispatchDropTable(event, dropTableResult.table, dropTableResult.result.ShardVersionUpdate[0]); err != nil {
		procedure.CancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", dropTableResult.table.Name))
		return
	}
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

type DropTableMetaDataResult struct {
	table  storage.Table
	result metadata.DropTableResult
	exists bool
}

func dropTableMetaData(event *fsm.Event, tableName string) (DropTableMetaDataResult, error) {
	request, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		return DropTableMetaDataResult{
			table:  storage.Table{},
			result: metadata.DropTableResult{},
			exists: false,
		}, errors.WithMessage(err, "get request from event")
	}

	table, exists, err := request.p.params.ClusterMetadata.GetTable(request.schemaName(), tableName)
	if err != nil {
		return DropTableMetaDataResult{
			table:  storage.Table{},
			result: metadata.DropTableResult{},
			exists: false,
		}, errors.WithMessage(err, "cluster get table")
	}
	if !exists {
		log.Warn("drop non-existing table", zap.String("schema", request.schemaName()), zap.String("table", tableName))
		return DropTableMetaDataResult{storage.Table{}, metadata.DropTableResult{}, false}, nil
	}

	result, err := request.p.params.ClusterMetadata.DropTable(request.ctx, request.schemaName(), tableName)
	if err != nil {
		return DropTableMetaDataResult{storage.Table{}, metadata.DropTableResult{}, false}, errors.WithMessage(err, "cluster drop table")
	}
	return DropTableMetaDataResult{
		table:  table,
		result: result,
		exists: true,
	}, nil
}

func dispatchDropTable(event *fsm.Event, table storage.Table, version metadata.ShardVersionUpdate) error {
	request, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		return errors.WithMessage(err, "get request from event")
	}
	shardNodes, err := request.p.params.ClusterMetadata.GetShardNodesByShardID(version.ShardID)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get shard nodes by shard id")
		return errors.WithMessage(err, "cluster get shard by shard id")
	}

	tableInfo := metadata.TableInfo{
		ID:         table.ID,
		Name:       table.Name,
		SchemaID:   table.SchemaID,
		SchemaName: request.p.params.SourceReq.GetSchemaName(),
	}

	for _, shardNode := range shardNodes {
		err = request.p.params.Dispatch.DropTableOnShard(request.ctx, shardNode.NodeName, eventdispatch.DropTableOnShardRequest{
			UpdateShardInfo: eventdispatch.UpdateShardInfo{
				CurrShardInfo: metadata.ShardInfo{
					ID:      version.ShardID,
					Role:    storage.ShardRoleLeader,
					Version: version.CurrVersion,
				},
				PrevVersion: version.PrevVersion,
			},
			TableInfo: tableInfo,
		})
		if err != nil {
			return errors.WithMessage(err, "dispatch drop table on shard")
		}
	}

	return nil
}
