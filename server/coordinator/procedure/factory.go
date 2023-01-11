// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const defaultPartitionTableNum = 1

type Factory struct {
	idAllocator    id.Allocator
	dispatch       eventdispatch.Dispatch
	storage        Storage
	clusterManager cluster.Manager
	shardPicker    ShardPicker

	// TODO: This is a temporary implementation version, which needs to be refined to the table level later.
	partitionTableProportionOfNodes float32
}

type ScatterRequest struct {
	Cluster  *cluster.Cluster
	ShardIDs []storage.ShardID
}

type CreateTableRequest struct {
	Cluster   *cluster.Cluster
	SourceReq *metaservicepb.CreateTableRequest

	OnSucceeded func(cluster.CreateTableResult) error
	OnFailed    func(error) error
}

func (request *CreateTableRequest) isPartitionTable() (bool, error) {
	if request.SourceReq.PartitionTableInfo != nil {
		if len(request.SourceReq.PartitionTableInfo.SubTableNames) == 0 {
			log.Error("fail to create table", zap.Error(ErrEmptyPartitionNames))
			return false, ErrEmptyPartitionNames
		}
		return true, nil
	}
	return false, nil
}

type DropTableRequest struct {
	Cluster   *cluster.Cluster
	SourceReq *metaservicepb.DropTableRequest

	OnSucceeded func(cluster.TableInfo) error
	OnFailed    func(error) error
}

func (d DropTableRequest) IsPartitionTable() bool {
	return d.SourceReq.PartitionTableInfo != nil
}

type TransferLeaderRequest struct {
	ClusterName       string
	ShardID           storage.ShardID
	OldLeaderNodeName string
	NewLeaderNodeName string
	ClusterVersion    uint64
}

type SplitRequest struct {
	ClusterName    string
	SchemaName     string
	TableNames     []string
	ShardID        storage.ShardID
	NewShardID     storage.ShardID
	TargetNodeName string
	ClusterVersion uint64
}

type CreatePartitionTableRequest struct {
	Cluster   *cluster.Cluster
	SourceReq *metaservicepb.CreateTableRequest

	PartitionTableRatioOfNodes float32

	OnSucceeded func(cluster.CreateTableResult) error
	OnFailed    func(error) error
}

func NewFactory(allocator id.Allocator, dispatch eventdispatch.Dispatch, storage Storage, manager cluster.Manager, partitionTableProportionOfNodes float32) *Factory {
	return &Factory{
		idAllocator:                     allocator,
		dispatch:                        dispatch,
		storage:                         storage,
		clusterManager:                  manager,
		shardPicker:                     NewRandomBalancedShardPicker(manager),
		partitionTableProportionOfNodes: partitionTableProportionOfNodes,
	}
}

func (f *Factory) CreateScatterProcedure(ctx context.Context, request ScatterRequest) (Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, err
	}
	procedure := NewScatterProcedure(f.dispatch, request.Cluster, id, request.ShardIDs)
	return procedure, nil
}

func (f *Factory) MakeCreateTableProcedure(ctx context.Context, request CreateTableRequest) (Procedure, error) {
	isPartitionTable, err := request.isPartitionTable()
	if err != nil {
		return nil, err
	}

	if isPartitionTable {
		return f.makeCreatePartitionTableProcedure(ctx, CreatePartitionTableRequest{
			Cluster:                    request.Cluster,
			SourceReq:                  request.SourceReq,
			PartitionTableRatioOfNodes: f.partitionTableProportionOfNodes,
			OnSucceeded:                request.OnSucceeded,
			OnFailed:                   request.OnFailed,
		})
	}

	return f.makeCreateTableProcedure(ctx, request)
}

func (f *Factory) makeCreateTableProcedure(ctx context.Context, request CreateTableRequest) (Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, err
	}
	shards, err := f.shardPicker.PickShards(ctx, request.Cluster.Name(), 1, false)
	if err != nil {
		log.Error("pick table shard", zap.Error(err))
		return nil, errors.WithMessage(err, "pick table shard")
	}
	if len(shards) != 1 {
		log.Error("pick table shards length not equal 1", zap.Int("shards", len(shards)))
		return nil, errors.WithMessagef(ErrPickShard, "pick table shard, shards length:%d", len(shards))
	}

	procedure := NewCreateTableProcedure(CreateTableProcedureRequest{
		Dispatch:    f.dispatch,
		Cluster:     request.Cluster,
		ID:          id,
		ShardID:     shards[0].ShardInfo.ID,
		Req:         request.SourceReq,
		OnSucceeded: request.OnSucceeded,
		OnFailed:    request.OnFailed,
	})
	return procedure, nil
}

func (f *Factory) makeCreatePartitionTableProcedure(ctx context.Context, request CreatePartitionTableRequest) (Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, err
	}

	getNodeShardResult, err := request.Cluster.GetNodeShards(ctx)
	if err != nil {
		log.Error("cluster get node shard result")
		return nil, err
	}

	nodeNames := make(map[string]int)
	for _, nodeShard := range getNodeShardResult.NodeShards {
		nodeNames[nodeShard.ShardNode.NodeName] = 1
	}

	partitionTableNum := Max(defaultPartitionTableNum, int(float32(len(nodeNames))*request.PartitionTableRatioOfNodes))

	partitionTableShards, err := f.shardPicker.PickShards(ctx, request.Cluster.Name(), partitionTableNum, false)
	if err != nil {
		return nil, errors.WithMessage(err, "pick partition table shards")
	}

	dataTableShards, err := f.shardPicker.PickShards(ctx, request.Cluster.Name(), len(request.SourceReq.PartitionTableInfo.SubTableNames), true)
	if err != nil {
		return nil, errors.WithMessage(err, "pick data table shards")
	}

	procedure := NewCreatePartitionTableProcedure(CreatePartitionTableProcedureRequest{
		id:                   id,
		cluster:              request.Cluster,
		dispatch:             f.dispatch,
		storage:              f.storage,
		req:                  request.SourceReq,
		partitionTableShards: partitionTableShards,
		dataTablesShards:     dataTableShards,
		onSucceeded:          request.OnSucceeded,
		onFailed:             request.OnFailed,
	})
	return procedure, nil
}

func (f *Factory) CreateDropTableProcedure(ctx context.Context, request DropTableRequest) (Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, err
	}

	if request.IsPartitionTable() {
		req := DropPartitionTableProcedureRequest{
			ID:          id,
			Cluster:     request.Cluster,
			Dispatch:    f.dispatch,
			Storage:     f.storage,
			Request:     request.SourceReq,
			OnSucceeded: request.OnSucceeded,
			OnFailed:    request.OnFailed,
		}
		procedure := NewDropPartitionTableProcedure(req)
		return procedure, nil
	}

	procedure := NewDropTableProcedure(f.dispatch, request.Cluster, id,
		request.SourceReq, request.OnSucceeded, request.OnFailed)
	return procedure, nil
}

func (f *Factory) CreateTransferLeaderProcedure(ctx context.Context, request TransferLeaderRequest) (Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, err
	}

	c, err := f.clusterManager.GetCluster(ctx, request.ClusterName)
	if err != nil {
		log.Error("cluster not found", zap.String("clusterName", request.ClusterName))
		return nil, cluster.ErrClusterNotFound
	}

	return NewTransferLeaderProcedure(f.dispatch, c, f.storage,
		request.ShardID, request.OldLeaderNodeName, request.NewLeaderNodeName, id)
}

func (f *Factory) CreateSplitProcedure(ctx context.Context, request SplitRequest) (Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, err
	}

	c, err := f.clusterManager.GetCluster(ctx, request.ClusterName)
	if err != nil {
		log.Error("cluster not found", zap.String("clusterName", request.ClusterName))
		return nil, cluster.ErrClusterNotFound
	}

	procedure := NewSplitProcedure(id, f.dispatch, f.storage, c, request.SchemaName, request.ShardID, request.NewShardID, request.TableNames, request.TargetNodeName)
	return procedure, nil
}

func (f *Factory) allocProcedureID(ctx context.Context) (uint64, error) {
	id, err := f.idAllocator.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessage(err, "alloc procedure id")
	}
	return id, nil
}
