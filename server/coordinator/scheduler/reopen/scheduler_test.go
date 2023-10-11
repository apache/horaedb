// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package reopen_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/coordinator/scheduler/reopen"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestReopenShardScheduler(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	procedureFactory := coordinator.NewFactory(zap.NewNop(), test.MockIDAllocator{}, test.MockDispatch{}, test.NewTestStorage(t))

	s := reopen.NewShardScheduler(procedureFactory, 1)

	emptyCluster := test.InitEmptyCluster(ctx, t)
	// ReopenShardScheduler should not schedule when cluster is not stable.
	result, err := s.Schedule(ctx, emptyCluster.GetMetadata().GetClusterSnapshot())
	re.NoError(err)
	re.Nil(result.Procedure)

	stableCluster := test.InitStableCluster(ctx, t)
	snapshot := stableCluster.GetMetadata().GetClusterSnapshot()

	// Add shard with ready status.
	snapshot.RegisteredNodes[0].ShardInfos = append(snapshot.RegisteredNodes[0].ShardInfos, metadata.ShardInfo{
		ID:      0,
		Role:    storage.ShardRoleLeader,
		Version: 0,
		Status:  storage.ShardStatusReady,
	})
	re.NoError(err)
	re.Nil(result.Procedure)

	// Add shard with partitionOpen status.
	snapshot.RegisteredNodes[0].ShardInfos = append(snapshot.RegisteredNodes[0].ShardInfos, metadata.ShardInfo{
		ID:      1,
		Role:    storage.ShardRoleLeader,
		Version: 0,
		Status:  storage.ShardStatusPartialOpen,
	})
	result, err = s.Schedule(ctx, snapshot)
	re.NoError(err)
	re.NotNil(result.Procedure)
}
