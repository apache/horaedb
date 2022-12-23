// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"go.uber.org/zap"
)

// CreateTableProcedure is a proxy procedure, it determines the actual procedure created according to the request type.
type CreateTableProcedure struct {
	realProcedure Procedure
}

func NewCreateTableProcedure(ctx context.Context, factory *Factory, c *cluster.Cluster, sourceReq *metaservicepb.CreateTableRequest, onSucceeded func(cluster.CreateTableResult) error, onFailed func(error) error) (Procedure, error) {
	if sourceReq.PartitionInfo != nil && len(sourceReq.PartitionInfo.GetNames()) == 0 {
		log.Error("fail to create table", zap.Error(ErrEmptyPartitionNames))
		return CreateTableProcedure{}, ErrEmptyPartitionNames
	}

	var realProcedure Procedure
	if sourceReq.PartitionInfo != nil && len(sourceReq.PartitionInfo.GetNames()) != 0 {
		p, err := factory.makeCreatePartitionTableProcedure(ctx, CreatePartitionTableRequest{
			ClusterName: c.Name(),
			SourceReq:   sourceReq,
			OnSucceeded: onSucceeded,
			OnFailed:    onFailed,
		})
		if err != nil {
			log.Error("fail to create partition table", zap.Error(err))
			return CreateTableProcedure{}, err
		}
		realProcedure = p
	} else {
		p, err := factory.makeCreateNormalTableProcedure(ctx, CreateTableRequest{
			Cluster:     c,
			SourceReq:   sourceReq,
			OnSucceeded: onSucceeded,
			OnFailed:    onFailed,
		})
		if err != nil {
			log.Error("fail to create table", zap.Error(err))
			return CreateTableProcedure{}, err
		}
		realProcedure = p
	}

	return CreateTableProcedure{
		realProcedure: realProcedure,
	}, nil
}

func (p CreateTableProcedure) ID() uint64 {
	return p.realProcedure.ID()
}

func (p CreateTableProcedure) Typ() Typ {
	return p.realProcedure.Typ()
}

func (p CreateTableProcedure) Start(ctx context.Context) error {
	return p.realProcedure.Start(ctx)
}

func (p CreateTableProcedure) Cancel(ctx context.Context) error {
	return p.realProcedure.Cancel(ctx)
}

func (p CreateTableProcedure) State() State {
	return p.realProcedure.State()
}
