// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/coordinator/scheduler"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestAssignSchedule(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	procedureFactory := coordinator.NewFactory(test.MockIDAllocator{}, test.MockDispatch{}, test.NewTestStorage(t))

	s := scheduler.NewAssignShardScheduler(procedureFactory, coordinator.NewUniformityConsistentHashNodePicker(zap.NewNop()), 1)

	// EmptyCluster would be scheduled an empty procedure.
	emptyCluster := test.InitEmptyCluster(ctx, t)
	result, err := s.Schedule(ctx, emptyCluster.GetMetadata().GetClusterSnapshot())
	re.NoError(err)
	re.Empty(result)

	// PrepareCluster would be scheduled a transfer leader procedure.
	prepareCluster := test.InitPrepareCluster(ctx, t)
	result, err = s.Schedule(ctx, prepareCluster.GetMetadata().GetClusterSnapshot())
	re.NoError(err)
	re.NotEmpty(result)

	// StableCluster with all shards assigned would be scheduled an empty procedure.
	stableCluster := test.InitStableCluster(ctx, t)
	result, err = s.Schedule(ctx, stableCluster.GetMetadata().GetClusterSnapshot())
	re.NoError(err)
	re.Empty(result)
}
