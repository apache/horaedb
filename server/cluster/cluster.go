// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/scheduler"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	defaultProcedurePrefixKey = "procedure"
	defaultAllocStep          = 5
)

type Cluster struct {
	logger   *zap.Logger
	metadata *metadata.ClusterMetadata

	procedureFactory *coordinator.Factory
	procedureManager procedure.Manager
	schedulerManager scheduler.Manager
}

func NewCluster(logger *zap.Logger, metadata *metadata.ClusterMetadata, client *clientv3.Client, rootPath string) (*Cluster, error) {
	procedureStorage := procedure.NewEtcdStorageImpl(client, rootPath)
	procedureManager, err := procedure.NewManagerImpl(logger, metadata)
	if err != nil {
		return nil, errors.WithMessage(err, "create procedure manager")
	}
	dispatch := eventdispatch.NewDispatchImpl()
	procedureFactory := coordinator.NewFactory(id.NewAllocatorImpl(logger, client, defaultProcedurePrefixKey, defaultAllocStep), dispatch, procedureStorage)

	schedulerManager := scheduler.NewManager(logger, procedureManager, procedureFactory, metadata, client, rootPath, metadata.GetEnableSchedule(), metadata.GetTopologyType())

	return &Cluster{
		logger:           logger,
		metadata:         metadata,
		procedureFactory: procedureFactory,
		procedureManager: procedureManager,
		schedulerManager: schedulerManager,
	}, nil
}

func (c *Cluster) GetMetadata() *metadata.ClusterMetadata {
	return c.metadata
}

func (c *Cluster) GetProcedureManager() procedure.Manager {
	return c.procedureManager
}

func (c *Cluster) GetProcedureFactory() *coordinator.Factory {
	return c.procedureFactory
}

func (c *Cluster) GetSchedulerManager() scheduler.Manager {
	return c.schedulerManager
}

func (c *Cluster) Start(ctx context.Context) error {
	if err := c.procedureManager.Start(ctx); err != nil {
		return errors.WithMessage(err, "start procedure manager")
	}
	if err := c.schedulerManager.Start(ctx); err != nil {
		return errors.WithMessage(err, "start scheduler manager")
	}
	return nil
}

func (c *Cluster) Stop(ctx context.Context) error {
	if err := c.procedureManager.Stop(ctx); err != nil {
		return errors.WithMessage(err, "stop procedure manager")
	}
	if err := c.schedulerManager.Stop(ctx); err != nil {
		return errors.WithMessage(err, "stop scheduler manager")
	}
	return nil
}
