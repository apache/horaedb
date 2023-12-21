/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cluster

import (
	"context"
	"strings"

	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/eventdispatch"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/scheduler/manager"
	"github.com/apache/incubator-horaedb-meta/server/id"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	defaultProcedurePrefixKey = "ProcedureID"
	defaultAllocStep          = 50
)

type Cluster struct {
	logger   *zap.Logger
	metadata *metadata.ClusterMetadata

	procedureFactory *coordinator.Factory
	procedureManager procedure.Manager
	schedulerManager manager.SchedulerManager
}

func NewCluster(logger *zap.Logger, metadata *metadata.ClusterMetadata, client *clientv3.Client, rootPath string) (*Cluster, error) {
	procedureStorage := procedure.NewEtcdStorageImpl(client, rootPath, uint32(metadata.GetClusterID()))
	procedureManager, err := procedure.NewManagerImpl(logger, metadata)
	if err != nil {
		return nil, errors.WithMessage(err, "create procedure manager")
	}
	dispatch := eventdispatch.NewDispatchImpl()

	procedureIDRootPath := strings.Join([]string{rootPath, metadata.Name(), defaultProcedurePrefixKey}, "/")
	procedureFactory := coordinator.NewFactory(logger, id.NewAllocatorImpl(logger, client, procedureIDRootPath, defaultAllocStep), dispatch, procedureStorage)

	schedulerManager := manager.NewManager(logger, procedureManager, procedureFactory, metadata, client, rootPath, metadata.GetTopologyType(), metadata.GetProcedureExecutingBatchSize())

	return &Cluster{
		logger:           logger,
		metadata:         metadata,
		procedureFactory: procedureFactory,
		procedureManager: procedureManager,
		schedulerManager: schedulerManager,
	}, nil
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

func (c *Cluster) GetMetadata() *metadata.ClusterMetadata {
	return c.metadata
}

func (c *Cluster) GetProcedureManager() procedure.Manager {
	return c.procedureManager
}

func (c *Cluster) GetProcedureFactory() *coordinator.Factory {
	return c.procedureFactory
}

func (c *Cluster) GetSchedulerManager() manager.SchedulerManager {
	return c.schedulerManager
}

func (c *Cluster) GetShards() []storage.ShardID {
	return c.metadata.GetShards()
}

func (c *Cluster) GetShardNodes() metadata.GetShardNodesResult {
	return c.metadata.GetShardNodes()
}
