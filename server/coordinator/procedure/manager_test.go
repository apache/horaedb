// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure_test

import (
	"context"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type MockProcedure struct {
	id                 uint64
	state              procedure.State
	relatedVersionInfo procedure.RelatedVersionInfo
	execTime           time.Duration
}

func (m *MockProcedure) ID() uint64 {
	return m.id
}

func (m *MockProcedure) Typ() procedure.Typ {
	return procedure.CreateTable
}

func (m *MockProcedure) Start(_ context.Context) error {
	m.state = procedure.StateRunning
	// Mock procedure execute time.
	time.Sleep(m.execTime)
	m.state = procedure.StateFinished
	return nil
}

func (m *MockProcedure) Cancel(_ context.Context) error {
	return nil
}

func (m *MockProcedure) State() procedure.State {
	return m.state
}

func (m *MockProcedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return m.relatedVersionInfo
}

func (m *MockProcedure) Priority() procedure.Priority {
	return procedure.PriorityMed
}

func TestManager(t *testing.T) {
	ctx := context.Background()
	re := require.New(t)

	c := test.InitStableCluster(ctx, t)
	manager, err := procedure.NewManagerImpl(zap.NewNop(), c.GetMetadata())
	re.NoError(err)

	err = manager.Start(ctx)
	re.NoError(err)

	procedureID := uint64(0)
	// Test submit multi single shard procedure.
	snapshot := c.GetMetadata().GetClusterSnapshot()
	for shardID, shardView := range snapshot.Topology.ShardViewsMapping {
		err = manager.Submit(ctx, &MockProcedure{
			id:                 procedureID,
			state:              procedure.StateInit,
			relatedVersionInfo: procedure.RelatedVersionInfo{ClusterID: c.GetMetadata().GetClusterID(), ShardWithVersion: map[storage.ShardID]uint64{shardID: shardView.Version}, ClusterVersion: c.GetMetadata().GetClusterViewVersion()},
			execTime:           time.Millisecond * 50,
		})
		procedureID++
		re.NoError(err)
	}
	// Procedures state is init, list result length should be 0.
	infos, err := manager.ListRunningProcedure(ctx)
	re.NoError(err)
	re.Equal(0, len(infos))
	time.Sleep(time.Millisecond * 10)
	// Procedures state is running, list result length should be len(shardViews).
	infos, err = manager.ListRunningProcedure(ctx)
	re.NoError(err)
	re.Equal(len(snapshot.Topology.ShardViewsMapping), len(infos))
	time.Sleep(time.Millisecond * 100)
	// Procedures state is finish, list result length should be 0.
	infos, err = manager.ListRunningProcedure(ctx)
	re.NoError(err)
	re.Equal(0, len(infos))

	{
		// Test schedule procedure with multi shards.
		// 1. Submit the procedure of all shard, and all shards are locked at this time.
		// 2. Submit procedures with single shard.
		// 3. Because of the earliest submitted procedure, all subsequent procedures cannot be executed at this time.
		// 4. Wait for the execution of the first procedure to complete, and the remaining procedures will be promoted and executed in parallel.
		// 5. All procedures are executed finish.

		// Test submit procedure with multi shards.
		shardWithVersions := map[storage.ShardID]uint64{}
		for id, view := range snapshot.Topology.ShardViewsMapping {
			shardWithVersions[id] = view.Version
		}
		err = manager.Submit(ctx, &MockProcedure{
			id:                 procedureID,
			state:              procedure.StateInit,
			relatedVersionInfo: procedure.RelatedVersionInfo{ClusterID: c.GetMetadata().GetClusterID(), ShardWithVersion: shardWithVersions, ClusterVersion: c.GetMetadata().GetClusterViewVersion()},
			execTime:           time.Millisecond * 100,
		})
		re.NoError(err)
		procedureID++
		for shardID, shardView := range snapshot.Topology.ShardViewsMapping {
			err = manager.Submit(ctx, &MockProcedure{
				id:                 procedureID,
				state:              procedure.StateInit,
				relatedVersionInfo: procedure.RelatedVersionInfo{ClusterID: c.GetMetadata().GetClusterID(), ShardWithVersion: map[storage.ShardID]uint64{shardID: shardView.Version}, ClusterVersion: c.GetMetadata().GetClusterViewVersion()},
				execTime:           time.Millisecond * 50,
			})
			procedureID++
			re.NoError(err)
		}

		// Procedure with multi shard is running, all shard is locked.
		time.Sleep(time.Millisecond * 10)
		infos, err = manager.ListRunningProcedure(ctx)
		re.NoError(err)
		re.Equal(len(snapshot.Topology.ShardViewsMapping), len(infos))

		// Procedure with multi shard is still running, all shard is still locked.
		time.Sleep(time.Millisecond * 50)
		infos, err = manager.ListRunningProcedure(ctx)
		re.NoError(err)
		re.Equal(len(snapshot.Topology.ShardViewsMapping), len(infos))

		// Procedure with multi shard is finished.
		time.Sleep(time.Millisecond * 50)
		infos, err = manager.ListRunningProcedure(ctx)
		re.NoError(err)
		re.Equal(0, len(infos))

		// Waiting for next promote, procedures with single shard will be scheduled.
		time.Sleep(time.Millisecond * 500)
		infos, err = manager.ListRunningProcedure(ctx)
		re.NoError(err)
		re.Equal(len(snapshot.Topology.ShardViewsMapping), len(infos))

		// procedures with single shard is finished.
		time.Sleep(time.Millisecond * 50)
		infos, err = manager.ListRunningProcedure(ctx)
		re.NoError(err)
		re.Equal(0, len(infos))

		err = manager.Stop(ctx)
		re.NoError(err)
	}
}
