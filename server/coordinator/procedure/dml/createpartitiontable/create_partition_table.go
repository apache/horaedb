// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package createpartitiontable

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// fsm state change:
// ┌────────┐     ┌──────────────────────┐     ┌────────────────────┐     ┌────────────────────┐     ┌───────────┐
// │ Begin  ├─────▶ CreatePartitionTable ├─────▶  CreateDataTables  ├─────▶OpenPartitionTables ├─────▶  Finish   │
// └────────┘     └──────────────────────┘     └────────────────────┘     └────────────────────┘     └───────────┘
const (
	eventCreatePartitionTable     = "EventCreatePartitionTable"
	eventCreateSubTables          = "EventCreateSubTables"
	eventUpdateTableShardMetadata = "EventUpdateTableShardMetadata"
	eventOpenPartitionTables      = "EventOpenPartitionTables"
	eventFinish                   = "EventFinish"

	stateBegin                    = "StateBegin"
	stateCreatePartitionTable     = "StateCreatePartitionTable"
	stateCreateSubTables          = "StateCreateSubTables"
	stateUpdateTableShardMetadata = "StateUpdateTableShardMetadata"
	stateOpenPartitionTables      = "StateOpenPartitionTables"
	stateFinish                   = "StateFinish"
)

var (
	createPartitionTableEvents = fsm.Events{
		{Name: eventCreatePartitionTable, Src: []string{stateBegin}, Dst: stateCreatePartitionTable},
		{Name: eventCreateSubTables, Src: []string{stateCreatePartitionTable}, Dst: stateCreateSubTables},
		{Name: eventUpdateTableShardMetadata, Src: []string{stateCreateSubTables}, Dst: stateUpdateTableShardMetadata},
		{Name: eventOpenPartitionTables, Src: []string{stateUpdateTableShardMetadata}, Dst: stateOpenPartitionTables},
		{Name: eventFinish, Src: []string{stateOpenPartitionTables}, Dst: stateFinish},
	}
	createPartitionTableCallbacks = fsm.Callbacks{
		eventCreatePartitionTable:     createPartitionTableCallback,
		eventCreateSubTables:          createDataTablesCallback,
		eventUpdateTableShardMetadata: openPartitionTableMetadataCallback,
		eventOpenPartitionTables:      openPartitionTableCallback,
		eventFinish:                   finishCallback,
	}
)

type Procedure struct {
	id       uint64
	fsm      *fsm.FSM
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch
	storage  procedure.Storage

	req *metaservicepb.CreateTableRequest

	partitionTableShards []cluster.ShardNodeWithVersion
	subTablesShards      []cluster.ShardNodeWithVersion

	onSucceeded func(cluster.CreateTableResult) error
	onFailed    func(error) error

	lock  sync.RWMutex
	state procedure.State
}

type ProcedureRequest struct {
	ID                   uint64
	Cluster              *cluster.Cluster
	Dispatch             eventdispatch.Dispatch
	Storage              procedure.Storage
	Req                  *metaservicepb.CreateTableRequest
	PartitionTableShards []cluster.ShardNodeWithVersion
	SubTablesShards      []cluster.ShardNodeWithVersion
	OnSucceeded          func(cluster.CreateTableResult) error
	OnFailed             func(error) error
}

func NewProcedure(req ProcedureRequest) *Procedure {
	fsm := fsm.NewFSM(
		stateBegin,
		createPartitionTableEvents,
		createPartitionTableCallbacks,
	)
	return &Procedure{
		id:                   req.ID,
		fsm:                  fsm,
		cluster:              req.Cluster,
		dispatch:             req.Dispatch,
		storage:              req.Storage,
		req:                  req.Req,
		partitionTableShards: req.PartitionTableShards,
		subTablesShards:      req.SubTablesShards,
		onSucceeded:          req.OnSucceeded,
		onFailed:             req.OnFailed,
	}
}

func (p *Procedure) ID() uint64 {
	return p.id
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.CreatePartitionTable
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	createPartitionTableRequest := &callbackRequest{
		ctx:                  ctx,
		cluster:              p.cluster,
		dispatch:             p.dispatch,
		sourceReq:            p.req,
		partitionTableShards: p.partitionTableShards,
		subTablesShards:      p.subTablesShards,
		onSucceeded:          p.onSucceeded,
		onFailed:             p.onFailed,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			if err := p.fsm.Event(eventCreatePartitionTable, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "create partition table")
			}
		case stateCreatePartitionTable:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			if err := p.fsm.Event(eventCreateSubTables, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "create data tables")
			}
		case stateCreateSubTables:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			if err := p.fsm.Event(eventUpdateTableShardMetadata, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "update table shard metadata")
			}
		case stateUpdateTableShardMetadata:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			if err := p.fsm.Event(eventOpenPartitionTables, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "open partition tables")
			}
		case stateOpenPartitionTables:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			if err := p.fsm.Event(eventFinish, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "finish")
			}
		case stateFinish:
			// TODO: The state update sequence here is inconsistent with the previous one. Consider reconstructing the state update logic of the state machine.
			p.updateStateWithLock(procedure.StateFinished)
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "create partition table procedure persist")
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

type callbackRequest struct {
	ctx      context.Context
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	sourceReq *metaservicepb.CreateTableRequest

	onSucceeded func(cluster.CreateTableResult) error
	onFailed    func(error) error

	createTableResult    cluster.CreateTableResult
	partitionTableShards []cluster.ShardNodeWithVersion
	subTablesShards      []cluster.ShardNodeWithVersion
	versions             []cluster.ShardVersionUpdate
}

// 1. Create partition table in target node.
func createPartitionTableCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	// Select first shard to create partition table.
	partTableShardNode := req.partitionTableShards[0]

	partInfo := req.sourceReq.GetPartitionTableInfo().GetPartitionInfo()
	createTableResult, err := procedure.CreateTableMetadata(req.ctx, req.cluster, req.sourceReq.GetSchemaName(), req.sourceReq.GetName(), partTableShardNode.ShardInfo.ID, partInfo)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "create table metadata")
		return
	}
	req.createTableResult = createTableResult

	if err = procedure.CreateTableOnShard(req.ctx, req.cluster, req.dispatch, partTableShardNode.ShardInfo.ID, procedure.BuildCreateTableRequest(createTableResult, req.sourceReq, partInfo)); err != nil {
		procedure.CancelEventWithLog(event, err, "dispatch create table on shard")
		return
	}
}

// 2. Create data tables in target nodes.
func createDataTablesCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	for i, subTableShard := range req.subTablesShards {
		createTableResult, err := procedure.CreateTableMetadata(req.ctx, req.cluster, req.sourceReq.GetSchemaName(), req.sourceReq.GetPartitionTableInfo().SubTableNames[i], subTableShard.ShardInfo.ID, nil)
		if err != nil {
			procedure.CancelEventWithLog(event, err, "create table metadata")
			return
		}

		if err = procedure.CreateTableOnShard(req.ctx, req.cluster, req.dispatch, subTableShard.ShardInfo.ID, procedure.BuildCreateTableRequest(createTableResult, req.sourceReq, nil)); err != nil {
			procedure.CancelEventWithLog(event, err, "dispatch create table on shard")
			return
		}
	}
}

// 3. Update table shard mapping.
func openPartitionTableMetadataCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	req.partitionTableShards = append(req.partitionTableShards[:0], req.partitionTableShards[1:]...)
	versions := make([]cluster.ShardVersionUpdate, 0, len(req.partitionTableShards))
	for _, partitionTableShard := range req.partitionTableShards {
		shardVersionUpdate, err := req.cluster.OpenTable(req.ctx,
			cluster.OpenTableRequest{
				SchemaName: req.sourceReq.SchemaName,
				TableName:  req.sourceReq.Name,
				ShardID:    partitionTableShard.ShardInfo.ID,
			})
		if err != nil {
			procedure.CancelEventWithLog(event, err, "open table")
			return
		}
		versions = append(versions, shardVersionUpdate)
	}
	req.versions = versions
}

// 4. Open table on target shard.
func openPartitionTableCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	table, exists, err := req.cluster.GetTable(req.sourceReq.SchemaName, req.sourceReq.Name)
	if err != nil {
		log.Error("get table", zap.Error(err))
		procedure.CancelEventWithLog(event, err, "get table")
		return
	}

	if !exists {
		procedure.CancelEventWithLog(event, err, "the table to be closed does not exist")
		return
	}

	for _, version := range req.versions {
		shardNodes, err := req.cluster.GetShardNodesByShardID(version.ShardID)
		if err != nil {
			procedure.CancelEventWithLog(event, err, "get shard nodes by shard id")
			return
		}

		for _, shardNode := range shardNodes {
			// Open partition table on target shard.
			if err := req.dispatch.OpenTableOnShard(req.ctx, shardNode.NodeName,
				eventdispatch.OpenTableOnShardRequest{
					UpdateShardInfo: eventdispatch.UpdateShardInfo{
						CurrShardInfo: cluster.ShardInfo{
							ID:      shardNode.ID,
							Role:    shardNode.ShardRole,
							Version: version.CurrVersion,
						},
						PrevVersion: version.PrevVersion,
					},
					TableInfo: cluster.TableInfo{
						ID:         table.ID,
						Name:       table.Name,
						SchemaID:   table.SchemaID,
						SchemaName: req.sourceReq.SchemaName,
						PartitionInfo: storage.PartitionInfo{
							Info: req.sourceReq.GetPartitionTableInfo().GetPartitionInfo(),
						},
					},
				}); err != nil {
				procedure.CancelEventWithLog(event, err, "open table on shard")
				return
			}
		}
	}
}

func finishCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	log.Info("create partition table finish")

	if err := req.onSucceeded(req.createTableResult); err != nil {
		procedure.CancelEventWithLog(event, err, "create partition table on succeeded")
		return
	}
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
	err = p.storage.CreateOrUpdate(ctx, meta)
	if err != nil {
		return errors.WithMessage(err, "createOrUpdate procedure storage")
	}
	return nil
}

type rawData struct {
	ID       uint64
	FsmState string
	State    procedure.State

	CreateTableResult    cluster.CreateTableResult
	PartitionTableShards []cluster.ShardNodeWithVersion
	SubTablesShards      []cluster.ShardNodeWithVersion
}

func (p *Procedure) convertToMeta() (procedure.Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := rawData{
		ID:                   p.id,
		FsmState:             p.fsm.Current(),
		State:                p.state,
		PartitionTableShards: p.partitionTableShards,
		SubTablesShards:      p.subTablesShards,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return procedure.Meta{}, procedure.ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%v, err:%v", p.id, err)
	}

	meta := procedure.Meta{
		ID:    p.id,
		Typ:   procedure.CreatePartitionTable,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
