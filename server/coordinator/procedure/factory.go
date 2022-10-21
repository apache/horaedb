// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/pkg/errors"
)

type Factory struct {
	idAllocator id.Allocator
	dispatch    eventdispatch.Dispatch
}

type ScatterRequest struct {
	Cluster  *cluster.Cluster
	ShardIDs []uint32
}

// nolint
type TransferLeaderRequest struct {
	Cluster   *cluster.Cluster
	OldLeader clusterpb.Shard
	NewLeader clusterpb.Shard
}

type CreateTableRequest struct {
	Cluster *cluster.Cluster
	req     *metaservicepb.CreateTableRequest

	// TODO: correct callback input params
	onSuccess func() error
	onFailed  func() error
}

// nolint
func NewFactory(allocator id.Allocator, dispatch eventdispatch.Dispatch) *Factory {
	return &Factory{
		idAllocator: allocator,
		dispatch:    dispatch,
	}
}

func (f *Factory) CreateScatterProcedure(ctx context.Context, request *ScatterRequest) (Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "alloc procedure id")
	}
	procedure := NewScatterProcedure(f.dispatch, request.Cluster, id, request.ShardIDs)
	return procedure, nil
}

func (f *Factory) CreateTransferLeaderProcedure(ctx context.Context, request *TransferLeaderRequest) (Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "alloc procedure id")
	}
	procedure := NewTransferLeaderProcedure(f.dispatch, request.Cluster, &request.OldLeader, &request.NewLeader, id)
	return procedure, nil
}

func (f *Factory) CreateCreateTableProcedure(ctx context.Context, request *CreateTableRequest) (Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "alloc procedure id")
	}
	procedure := NewCreateTableProcedure(f.dispatch, request.Cluster, id,
		request.req, request.onSuccess, request.onFailed)
	return procedure, nil
}

func (f *Factory) allocProcedureID(ctx context.Context) (uint64, error) {
	return f.idAllocator.Alloc(ctx)
}
