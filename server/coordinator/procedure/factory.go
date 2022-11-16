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

type Factory struct {
	idAllocator    id.Allocator
	dispatch       eventdispatch.Dispatch
	storage        Storage
	clusterManager cluster.Manager
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

type DropTableRequest struct {
	Cluster   *cluster.Cluster
	SourceReq *metaservicepb.DropTableRequest

	OnSucceeded func(cluster.TableInfo) error
	OnFailed    func(error) error
}

type TransferLeaderRequest struct {
	ClusterName       string
	ShardID           storage.ShardID
	OldLeaderNodeName string
	NewLeaderNodeName string
}

func NewFactory(allocator id.Allocator, dispatch eventdispatch.Dispatch, storage Storage) *Factory {
	return &Factory{
		idAllocator: allocator,
		dispatch:    dispatch,
		storage:     storage,
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

func (f *Factory) CreateCreateTableProcedure(ctx context.Context, request CreateTableRequest) (Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, err
	}
	procedure := NewCreateTableProcedure(f.dispatch, request.Cluster, id,
		request.SourceReq, request.OnSucceeded, request.OnFailed)
	return procedure, nil
}

func (f *Factory) CreateDropTableProcedure(ctx context.Context, request DropTableRequest) (Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, err
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

func (f *Factory) allocProcedureID(ctx context.Context) (uint64, error) {
	id, err := f.idAllocator.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessage(err, "alloc procedure id")
	}
	return id, nil
}
