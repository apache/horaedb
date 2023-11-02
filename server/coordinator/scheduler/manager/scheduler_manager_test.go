/*
 * Copyright 2022 The CeresDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package manager_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/coordinator/scheduler/manager"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSchedulerManager(t *testing.T) {
	ctx := context.Background()
	re := require.New(t)

	// Init dependencies for scheduler manager.
	c := test.InitStableCluster(ctx, t)
	procedureManager, err := procedure.NewManagerImpl(zap.NewNop(), c.GetMetadata())
	re.NoError(err)
	dispatch := test.MockDispatch{}
	allocator := test.MockIDAllocator{}
	s := test.NewTestStorage(t)
	f := coordinator.NewFactory(zap.NewNop(), allocator, dispatch, s)
	_, client, _ := etcdutil.PrepareEtcdServerAndClient(t)

	// Create scheduler manager with enableScheduler equal to false.
	schedulerManager := manager.NewManager(zap.NewNop(), procedureManager, f, c.GetMetadata(), client, "/rootPath", false, storage.TopologyTypeStatic, 1)
	err = schedulerManager.Start(ctx)
	re.NoError(err)
	err = schedulerManager.Stop(ctx)
	re.NoError(err)

	// Create scheduler manager with static topology.
	schedulerManager = manager.NewManager(zap.NewNop(), procedureManager, f, c.GetMetadata(), client, "/rootPath", true, storage.TopologyTypeStatic, 1)
	err = schedulerManager.Start(ctx)
	re.NoError(err)
	schedulers := schedulerManager.ListScheduler()
	re.Equal(2, len(schedulers))
	err = schedulerManager.Stop(ctx)
	re.NoError(err)

	// Create scheduler manager with dynamic topology.
	schedulerManager = manager.NewManager(zap.NewNop(), procedureManager, f, c.GetMetadata(), client, "/rootPath", true, storage.TopologyTypeDynamic, 1)
	err = schedulerManager.Start(ctx)
	re.NoError(err)
	schedulers = schedulerManager.ListScheduler()
	re.Equal(2, len(schedulers))
	err = schedulerManager.Stop(ctx)
	re.NoError(err)
}
