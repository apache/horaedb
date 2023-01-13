// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
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
	eventDropDataTable            = "EventDropDataTable"
	eventDropPartitionTable       = "EventDropPartitionTable"
	eventClosePartitionTables     = "EventClosePartitionTables"
	eventDropPartitionTableFinish = "EventDropPartitionTableFinish"

	stateDropPartitionTableBegin  = "StateDropPartitionBegin"
	stateDropDataTable            = "StateDropDataTable"
	stateDropPartitionTable       = "StateDropPartitionTable"
	stateClosePartitionTables     = "StateClosePartitionTables"
	stateDropPartitionTableFinish = "StateDropPartitionTableFinish"
)

var (
	createDropPartitionTableEvents = fsm.Events{
		{Name: eventDropDataTable, Src: []string{stateDropPartitionTableBegin}, Dst: stateDropDataTable},
		{Name: eventDropPartitionTable, Src: []string{stateDropDataTable}, Dst: stateDropPartitionTable},
		{Name: eventClosePartitionTables, Src: []string{stateDropPartitionTable}, Dst: stateClosePartitionTables},
		{Name: eventDropPartitionTableFinish, Src: []string{stateClosePartitionTables}, Dst: stateDropPartitionTableFinish},
	}
	createDropPartitionTableCallbacks = fsm.Callbacks{
		eventDropDataTable:            dropDataTablesCallback,
		eventDropPartitionTable:       dropPartitionTableCallback,
		eventClosePartitionTables:     closePartitionTableCallback,
		eventDropPartitionTableFinish: finishDropPartitionTableCallback,
	}
)

type DropPartitionTableProcedure struct {
	id       uint64
	fsm      *fsm.FSM
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch
	storage  Storage

	onSucceeded func(cluster.TableInfo) error
	onFailed    func(error) error

	request *metaservicepb.DropTableRequest

	// Protect the state.
	lock  sync.RWMutex
	state State
}

type DropPartitionTableProcedureRequest struct {
	ID          uint64
	Cluster     *cluster.Cluster
	Dispatch    eventdispatch.Dispatch
	Storage     Storage
	Request     *metaservicepb.DropTableRequest
	OnSucceeded func(result cluster.TableInfo) error
	OnFailed    func(error) error
}

func NewDropPartitionTableProcedure(request DropPartitionTableProcedureRequest) *DropPartitionTableProcedure {
	fsm := fsm.NewFSM(
		stateDropPartitionTableBegin,
		createDropPartitionTableEvents,
		createDropPartitionTableCallbacks,
	)
	return &DropPartitionTableProcedure{
		id:          request.ID,
		fsm:         fsm,
		cluster:     request.Cluster,
		dispatch:    request.Dispatch,
		storage:     request.Storage,
		request:     request.Request,
		onSucceeded: request.OnSucceeded,
		onFailed:    request.OnFailed,
	}
}

func (p *DropPartitionTableProcedure) ID() uint64 {
	return p.id
}

func (p *DropPartitionTableProcedure) Typ() Typ {
	return DropPartitionTable
}

func (p *DropPartitionTableProcedure) Start(ctx context.Context) error {
	p.updateStateWithLock(StateRunning)

	dropPartitionTableRequest := &dropPartitionTableCallbackRequest{
		ctx:         ctx,
		cluster:     p.cluster,
		dispatch:    p.dispatch,
		request:     p.request,
		onSucceeded: p.onSucceeded,
		onFailed:    p.onFailed,
	}

	for {
		switch p.fsm.Current() {
		case stateDropPartitionTableBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventDropDataTable, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessage(err, "drop partition table procedure")
			}
		case stateDropDataTable:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventDropPartitionTable, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessage(err, "drop partition table procedure drop data table")
			}
		case stateDropPartitionTable:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventClosePartitionTables, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessage(err, "drop partition table procedure drop partition table")
			}
		case stateClosePartitionTables:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventDropPartitionTableFinish, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessage(err, "drop partition table procedure close partition tables")
			}
		case stateDropPartitionTableFinish:
			p.updateStateWithLock(StateFinished)
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			return nil
		}
	}
}

func (p *DropPartitionTableProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(StateCancelled)
	return nil
}

func (p *DropPartitionTableProcedure) State() State {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.state
}

func (p *DropPartitionTableProcedure) updateStateWithLock(state State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}

func (p *DropPartitionTableProcedure) persist(ctx context.Context) error {
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

func (p *DropPartitionTableProcedure) convertToMeta() (Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := dropPartitionTableRawData{
		ID:               p.id,
		FsmState:         p.fsm.Current(),
		State:            p.state,
		DropTableRequest: p.request,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return Meta{}, ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%d, err:%v", p.id, err)
	}

	meta := Meta{
		ID:    p.id,
		Typ:   DropPartitionTable,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}

type dropPartitionTableRawData struct {
	ID       uint64
	FsmState string
	State    State

	DropTableRequest *metaservicepb.DropTableRequest
}

type dropPartitionTableCallbackRequest struct {
	ctx      context.Context
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	onSucceeded func(info cluster.TableInfo) error
	onFailed    func(error) error

	request  *metaservicepb.DropTableRequest
	versions []cluster.ShardVersionUpdate
	table    storage.Table
}

func (d *dropPartitionTableCallbackRequest) schemaName() string {
	return d.request.GetSchemaName()
}

func (d *dropPartitionTableCallbackRequest) tableName() string {
	return d.request.GetName()
}

// 1. Drop data tables in target nodes.
func dropDataTablesCallback(event *fsm.Event) {
	req, err := getRequestFromEvent[*dropPartitionTableCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	if len(req.request.PartitionTableInfo.SubTableNames) == 0 {
		cancelEventWithLog(event, ErrEmptyPartitionNames, fmt.Sprintf("drop table, table:%s", req.request.Name))
		return
	}

	for _, tableName := range req.request.PartitionTableInfo.SubTableNames {
		table, dropTableResult, exists, err := dropTableMetaData(event, tableName)
		if err != nil {
			cancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", tableName))
			return
		}

		if !exists {
			continue
		}

		if len(dropTableResult.ShardVersionUpdate) != 1 {
			cancelEventWithLog(event, ErrDropTableResult, fmt.Sprintf("legnth of shardVersionResult!=1, current is %d", len(dropTableResult.ShardVersionUpdate)))
			return
		}

		if err := dispatchDropTable(event, table, dropTableResult.ShardVersionUpdate[0]); err != nil {
			cancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", tableName))
			return
		}
	}
}

// 2. Drop partition table in target node.
func dropPartitionTableCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[*dropPartitionTableCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	table, dropTableRet, exists, err := dropTableMetaData(event, request.tableName())
	if err != nil {
		cancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", request.tableName()))
		return
	}
	if !exists {
		cancelEventWithLog(event, ErrTableNotExists, fmt.Sprintf("table:%s", request.tableName()))
		return
	}

	if len(dropTableRet.ShardVersionUpdate) == 0 {
		cancelEventWithLog(event, ErrDropTableResult, fmt.Sprintf("legnth of shardVersionResult need >=1, current is %d", len(dropTableRet.ShardVersionUpdate)))
		return
	}

	request.versions = dropTableRet.ShardVersionUpdate
	request.table = table

	// Drop table in the first shard.
	if err := dispatchDropTable(event, table, dropTableRet.ShardVersionUpdate[0]); err != nil {
		cancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", table.Name))
		return
	}
}

// 3. Close partition table in target node.
func closePartitionTableCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[*dropPartitionTableCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	tableInfo := cluster.TableInfo{
		ID:         request.table.ID,
		Name:       request.table.Name,
		SchemaID:   request.table.SchemaID,
		SchemaName: request.request.GetSchemaName(),
	}

	for _, version := range request.versions[1:] {
		shardNodes, err := request.cluster.GetShardNodesByShardID(version.ShardID)
		if err != nil {
			cancelEventWithLog(event, err, "get shard nodes by shard id")
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
				cancelEventWithLog(event, err, "close shard")
				return
			}
		}
	}
}

func finishDropPartitionTableCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[*dropPartitionTableCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}
	log.Info("drop partition table finish")

	tableInfo := cluster.TableInfo{
		ID:         request.table.ID,
		Name:       request.table.Name,
		SchemaID:   request.table.SchemaID,
		SchemaName: request.request.GetSchemaName(),
	}

	if err = request.onSucceeded(tableInfo); err != nil {
		cancelEventWithLog(event, err, "drop partition table on succeeded")
		return
	}
}

func dropTableMetaData(event *fsm.Event, tableName string) (storage.Table, cluster.DropTableResult, bool, error) {
	request, err := getRequestFromEvent[*dropPartitionTableCallbackRequest](event)
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
	request, err := getRequestFromEvent[*dropPartitionTableCallbackRequest](event)
	if err != nil {
		return errors.WithMessage(err, "get request from event")
	}
	shardNodes, err := request.cluster.GetShardNodesByShardID(version.ShardID)
	if err != nil {
		cancelEventWithLog(event, err, "get shard nodes by shard id")
		return errors.WithMessage(err, "cluster get shard by shard id")
	}

	tableInfo := cluster.TableInfo{
		ID:         table.ID,
		Name:       table.Name,
		SchemaID:   table.SchemaID,
		SchemaName: request.request.GetSchemaName(),
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
