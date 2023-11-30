/*
 * Copyright 2022 The HoraeDB Authors
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

package reopen_test

import (
	"context"
	"testing"

	"github.com/CeresDB/horaemeta/server/cluster/metadata"
	"github.com/CeresDB/horaemeta/server/coordinator"
	"github.com/CeresDB/horaemeta/server/coordinator/procedure/test"
	"github.com/CeresDB/horaemeta/server/coordinator/scheduler/reopen"
	"github.com/CeresDB/horaemeta/server/storage"
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
