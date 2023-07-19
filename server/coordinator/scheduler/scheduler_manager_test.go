// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/coordinator/scheduler"
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
	f := coordinator.NewFactory(allocator, dispatch, s)
	_, client, _ := etcdutil.PrepareEtcdServerAndClient(t)

	// Create scheduler manager with enableScheduler equal to false.
	schedulerManager := scheduler.NewManager(zap.NewNop(), procedureManager, f, c.GetMetadata(), client, "/rootPath", false, storage.TopologyTypeStatic, 1)
	err = schedulerManager.Start(ctx)
	re.NoError(err)
	err = schedulerManager.Stop(ctx)
	re.NoError(err)

	// Create scheduler manager with static topology.
	schedulerManager = scheduler.NewManager(zap.NewNop(), procedureManager, f, c.GetMetadata(), client, "/rootPath", true, storage.TopologyTypeStatic, 1)
	err = schedulerManager.Start(ctx)
	re.NoError(err)
	schedulers := schedulerManager.ListScheduler()
	re.Equal(2, len(schedulers))
	err = schedulerManager.Stop(ctx)
	re.NoError(err)

	// Create scheduler manager with dynamic topology.
	schedulerManager = scheduler.NewManager(zap.NewNop(), procedureManager, f, c.GetMetadata(), client, "/rootPath", true, storage.TopologyTypeDynamic, 1)
	err = schedulerManager.Start(ctx)
	re.NoError(err)
	schedulers = schedulerManager.ListScheduler()
	re.Equal(3, len(schedulers))
	err = schedulerManager.Stop(ctx)
	re.NoError(err)
}
