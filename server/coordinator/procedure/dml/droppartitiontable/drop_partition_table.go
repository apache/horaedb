// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package droppartitiontable

import (
	"context"
	"encoding/json"
	"fmt"
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
// ┌────────┐     ┌────────────────┐     ┌────────────────────┐     ┌──────────────────────┐     ┌───────────┐
// │ Begin  ├─────▶  DropDataTable ├─────▶ DropPartitionTable ├─────▶ ClosePartitionTables ├─────▶  Finish   │
// └────────┘     └────────────────┘     └────────────────────┘     └──────────────────────┘     └───────────┘
const (
	eventDropDataTable        = "EventDropDataTable"
	eventDropPartitionTable   = "EventDropPartitionTable"
	eventClosePartitionTables = "EventClosePartitionTables"
	eventFinish               = "EventFinish"

	stateBegin                = "StateBegin"
	stateDropDataTable        = "StateDropDataTable"
	stateDropPartitionTable   = "StateDropPartitionTable"
	stateClosePartitionTables = "StateClosePartitionTables"
	stateFinish               = "StateFinish"
)

var (
	createDropPartitionTableEvents = fsm.Events{
		{Name: eventDropDataTable, Src: []string{stateBegin}, Dst: stateDropDataTable},
		{Name: eventDropPartitionTable, Src: []string{stateDropDataTable}, Dst: stateDropPartitionTable},
		{Name: eventClosePartitionTables, Src: []string{stateDropPartitionTable}, Dst: stateClosePartitionTables},
		{Name: eventFinish, Src: []string{stateClosePartitionTables}, Dst: stateFinish},
	}
	createDropPartitionTableCallbacks = fsm.Callbacks{
		eventDropDataTable:        dropDataTablesCallback,
		eventDropPartitionTable:   dropPartitionTableCallback,
		eventClosePartitionTables: closePartitionTableCallback,
		eventFinish:               finishCallback,
	}
)

type Procedure struct {
	id       uint64
	fsm      *fsm.FSM
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch
	storage  procedure.Storage

	onSucceeded func(cluster.TableInfo) error
	onFailed    func(error) error

	req *metaservicepb.DropTableRequest

	// Protect the state.
	lock  sync.RWMutex
	state procedure.State
}

type ProcedureRequest struct {
	ID          uint64
	Cluster     *cluster.Cluster
	Dispatch    eventdispatch.Dispatch
	Storage     procedure.Storage
	Request     *metaservicepb.DropTableRequest
	OnSucceeded func(result cluster.TableInfo) error
	OnFailed    func(error) error
}

func NewProcedure(req ProcedureRequest) *Procedure {
	fsm := fsm.NewFSM(
		stateBegin,
		createDropPartitionTableEvents,
		createDropPartitionTableCallbacks,
	)
	return &Procedure{
		id:          req.ID,
		fsm:         fsm,
		cluster:     req.Cluster,
		dispatch:    req.Dispatch,
		storage:     req.Storage,
		req:         req.Request,
		onSucceeded: req.OnSucceeded,
		onFailed:    req.OnFailed,
	}
}

func (p *Procedure) ID() uint64 {
	return p.id
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.DropPartitionTable
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	dropPartitionTableRequest := &callbackRequest{
		ctx:         ctx,
		cluster:     p.cluster,
		dispatch:    p.dispatch,
		req:         p.req,
		onSucceeded: p.onSucceeded,
		onFailed:    p.onFailed,
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
			if err := p.fsm.Event(eventClosePartitionTables, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "drop partition table procedure drop partition table")
			}
		case stateClosePartitionTables:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventFinish, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "drop partition table procedure close partition tables")
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
	err = p.storage.CreateOrUpdate(ctx, meta)
	if err != nil {
		return errors.WithMessage(err, "createOrUpdate procedure storage")
	}
	return nil
}

func (p *Procedure) convertToMeta() (procedure.Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := rawData{
		ID:               p.id,
		FsmState:         p.fsm.Current(),
		State:            p.state,
		DropTableRequest: p.req,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return procedure.Meta{}, procedure.ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%d, err:%v", p.id, err)
	}

	meta := procedure.Meta{
		ID:    p.id,
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
	ctx      context.Context
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	onSucceeded func(info cluster.TableInfo) error
	onFailed    func(error) error

	req      *metaservicepb.DropTableRequest
	versions []cluster.ShardVersionUpdate
	table    storage.Table
}

func (d *callbackRequest) schemaName() string {
	return d.req.GetSchemaName()
}

func (d *callbackRequest) tableName() string {
	return d.req.GetName()
}

// 1. Drop data tables in target nodes.
func dropDataTablesCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	if len(req.req.PartitionTableInfo.SubTableNames) == 0 {
		procedure.CancelEventWithLog(event, procedure.ErrEmptyPartitionNames, fmt.Sprintf("drop table, table:%s", req.req.Name))
		return
	}

	for _, tableName := range req.req.PartitionTableInfo.SubTableNames {
		table, dropTableResult, exists, err := dropTableMetaData(event, tableName)
		if err != nil {
			procedure.CancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", tableName))
			return
		}

		if !exists {
			continue
		}

		if len(dropTableResult.ShardVersionUpdate) != 1 {
			procedure.CancelEventWithLog(event, procedure.ErrDropTableResult, fmt.Sprintf("legnth of shardVersionResult!=1, current is %d", len(dropTableResult.ShardVersionUpdate)))
			return
		}

		if err := dispatchDropTable(event, table, dropTableResult.ShardVersionUpdate[0]); err != nil {
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

	table, dropTableRet, exists, err := dropTableMetaData(event, req.tableName())
	if err != nil {
		procedure.CancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", req.tableName()))
		return
	}
	if !exists {
		procedure.CancelEventWithLog(event, procedure.ErrTableNotExists, fmt.Sprintf("table:%s", req.tableName()))
		return
	}

	if len(dropTableRet.ShardVersionUpdate) == 0 {
		procedure.CancelEventWithLog(event, procedure.ErrDropTableResult, fmt.Sprintf("legnth of shardVersionResult need >=1, current is %d", len(dropTableRet.ShardVersionUpdate)))
		return
	}

	req.versions = dropTableRet.ShardVersionUpdate
	req.table = table

	// Drop table in the first shard.
	if err := dispatchDropTable(event, table, dropTableRet.ShardVersionUpdate[0]); err != nil {
		procedure.CancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", table.Name))
		return
	}
}

// 3. Close partition table in target node.
func closePartitionTableCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	tableInfo := cluster.TableInfo{
		ID:         request.table.ID,
		Name:       request.table.Name,
		SchemaID:   request.table.SchemaID,
		SchemaName: request.req.GetSchemaName(),
	}

	for _, version := range request.versions[1:] {
		shardNodes, err := request.cluster.GetShardNodesByShardID(version.ShardID)
		if err != nil {
			procedure.CancelEventWithLog(event, err, "get shard nodes by shard id")
			return
		}
		// Close partition table shard.
		for _, shardNode := range shardNodes {
			if err = request.dispatch.CloseTableOnShard(request.ctx, shardNode.NodeName, eventdispatch.CloseTableOnShardRequest{
				UpdateShardInfo: eventdispatch.UpdateShardInfo{CurrShardInfo: cluster.ShardInfo{
					ID:      shardNode.ID,
					Role:    shardNode.ShardRole,
					Version: version.CurrVersion,
				}, PrevVersion: version.PrevVersion},
				TableInfo: tableInfo,
			}); err != nil {
				procedure.CancelEventWithLog(event, err, "close shard")
				return
			}
		}
	}
}

func finishCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	log.Info("drop partition table finish")

	tableInfo := cluster.TableInfo{
		ID:         request.table.ID,
		Name:       request.table.Name,
		SchemaID:   request.table.SchemaID,
		SchemaName: request.req.GetSchemaName(),
	}

	if err = request.onSucceeded(tableInfo); err != nil {
		procedure.CancelEventWithLog(event, err, "drop partition table on succeeded")
		return
	}
}

func dropTableMetaData(event *fsm.Event, tableName string) (storage.Table, cluster.DropTableResult, bool, error) {
	request, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		return storage.Table{}, cluster.DropTableResult{}, false, errors.WithMessage(err, "get request from event")
	}

	table, exists, err := request.cluster.GetTable(request.schemaName(), tableName)
	if err != nil {
		return storage.Table{}, cluster.DropTableResult{}, false, errors.WithMessage(err, "cluster get table")
	}
	if !exists {
		log.Warn("drop non-existing table", zap.String("schema", request.schemaName()), zap.String("table", tableName))
		return storage.Table{}, cluster.DropTableResult{}, false, nil
	}

	result, err := request.cluster.DropTable(request.ctx, request.schemaName(), tableName)
	if err != nil {
		return storage.Table{}, cluster.DropTableResult{}, false, errors.WithMessage(err, "cluster drop table")
	}

	return table, result, true, nil
}

func dispatchDropTable(event *fsm.Event, table storage.Table, version cluster.ShardVersionUpdate) error {
	request, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		return errors.WithMessage(err, "get request from event")
	}
	shardNodes, err := request.cluster.GetShardNodesByShardID(version.ShardID)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get shard nodes by shard id")
		return errors.WithMessage(err, "cluster get shard by shard id")
	}

	tableInfo := cluster.TableInfo{
		ID:         table.ID,
		Name:       table.Name,
		SchemaID:   table.SchemaID,
		SchemaName: request.req.GetSchemaName(),
	}

	for _, shardNode := range shardNodes {
		err = request.dispatch.DropTableOnShard(request.ctx, shardNode.NodeName, eventdispatch.DropTableOnShardRequest{
			UpdateShardInfo: eventdispatch.UpdateShardInfo{
				CurrShardInfo: cluster.ShardInfo{
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
