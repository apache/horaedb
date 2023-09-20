// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/lock"
	"github.com/CeresDB/ceresmeta/server/storage"
	"go.uber.org/zap"
)

const (
	metaListBatchSize                = 100
	defaultWaitingQueueLen           = 1000
	defaultWaitingQueueDelay         = time.Millisecond * 500
	defaultPromoteDelay              = time.Millisecond * 100
	defaultProcedureWorkerChanBufSiz = 10
)

type ManagerImpl struct {
	logger   *zap.Logger
	metadata *metadata.ClusterMetadata

	// ProcedureShardLock is used to ensure the consistency of procedures' concurrent running on shard, that is to say, only one procedure is allowed to run on a specific shard.
	procedureShardLock *lock.EntryLock
	// All procedure will be put into waiting queue first, when runningProcedure is empty, try to promote some waiting procedures to new running procedures.
	waitingProcedures *DelayQueue
	// ProcedureWorkerChan is used to notify that a procedure has been submitted or completed, and the manager will perform promote after receiving the signal.
	procedureWorkerChan chan struct{}

	// This lock is used to protect the following fields.
	lock    sync.RWMutex
	running bool
	// There is only one procedure running for every shard.
	// It will be removed when the procedure is finished or failed.
	runningProcedures map[storage.ShardID]Procedure
}

func (m *ManagerImpl) Start(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.running {
		m.logger.Warn("procedure manager has already been started")
		return nil
	}

	m.procedureWorkerChan = make(chan struct{}, defaultProcedureWorkerChanBufSiz)
	go m.startProcedurePromote(ctx, m.procedureWorkerChan)

	m.running = true

	return nil
}

func (m *ManagerImpl) Stop(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, procedure := range m.runningProcedures {
		if procedure.State() == StateRunning {
			err := procedure.Cancel(ctx)
			m.logger.Error("cancel procedure failed", zap.Error(err), zap.Uint64("procedureID", procedure.ID()))
			// TODO: consider whether a single procedure cancel failed should return directly.
			return err
		}
	}

	m.running = false

	return nil
}

// TODO: Filter duplicate submitted Procedure.
func (m *ManagerImpl) Submit(_ context.Context, procedure Procedure) error {
	if err := m.waitingProcedures.Push(procedure, 0); err != nil {
		return err
	}

	select {
	case m.procedureWorkerChan <- struct{}{}:
	default:
	}

	return nil
}

func (m *ManagerImpl) ListRunningProcedure(_ context.Context) ([]*Info, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	procedureInfos := make([]*Info, 0, len(m.runningProcedures))
	for _, procedure := range m.runningProcedures {
		if procedure.State() == StateRunning {
			procedureInfos = append(procedureInfos, &Info{
				ID:    procedure.ID(),
				Typ:   procedure.Typ(),
				State: procedure.State(),
			})
		}
	}
	return procedureInfos, nil
}

func NewManagerImpl(logger *zap.Logger, metadata *metadata.ClusterMetadata) (Manager, error) {
	entryLock := lock.NewEntryLock(10)
	manager := &ManagerImpl{
		logger:             logger,
		metadata:           metadata,
		runningProcedures:  map[storage.ShardID]Procedure{},
		procedureShardLock: &entryLock,
		waitingProcedures:  NewProcedureDelayQueue(defaultWaitingQueueLen),
	}
	return manager, nil
}

func (m *ManagerImpl) startProcedurePromote(ctx context.Context, procedureWorkerChan chan struct{}) {
	ticker := time.NewTicker(defaultPromoteDelay)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.startProcedurePromoteInternal(ctx, procedureWorkerChan)
		case <-procedureWorkerChan:
			m.startProcedurePromoteInternal(ctx, procedureWorkerChan)
		case <-ctx.Done():
			return
		}
	}
}

func (m *ManagerImpl) startProcedurePromoteInternal(ctx context.Context, procedureWorkerChan chan struct{}) {
	newProcedures, err := m.promoteProcedure(ctx)
	if err != nil {
		m.logger.Error("promote procedure failed", zap.Error(err))
		return
	}

	m.lock.Lock()
	for _, newProcedure := range newProcedures {
		for shardID := range newProcedure.RelatedVersionInfo().ShardWithVersion {
			m.runningProcedures[shardID] = newProcedure
		}
	}
	m.lock.Unlock()

	for _, newProcedure := range newProcedures {
		m.logger.Info("promote procedure", zap.Uint64("procedureID", newProcedure.ID()))
		m.startProcedureWorker(ctx, newProcedure, procedureWorkerChan)
	}
}

func (m *ManagerImpl) startProcedureWorker(ctx context.Context, newProcedure Procedure, procedureWorkerChan chan struct{}) {
	go func() {
		start := time.Now()
		m.logger.Info("procedure start", zap.Uint64("procedureID", newProcedure.ID()))
		err := newProcedure.Start(ctx)
		if err != nil {
			m.logger.Error("procedure start failed", zap.Error(err), zap.Int64("costTime", time.Since(start).Milliseconds()))
		} else {
			m.logger.Info("procedure start finish", zap.Uint64("procedureID", newProcedure.ID()), zap.Int64("costTime", time.Since(start).Milliseconds()))
		}
		for shardID := range newProcedure.RelatedVersionInfo().ShardWithVersion {
			m.lock.Lock()
			delete(m.runningProcedures, shardID)
			m.lock.Unlock()

			m.procedureShardLock.UnLock([]uint64{uint64(shardID)})
		}
		select {
		case procedureWorkerChan <- struct{}{}:
		default:
		}
	}()
}

// Whether a waiting procedure could be running procedure.
func checkValid(p Procedure, clusterMetadata *metadata.ClusterMetadata) bool {
	// ClusterVersion and ShardVersion in this procedure must be same with current cluster topology.
	snapshot := clusterMetadata.GetClusterSnapshot()
	curClusterVersion := snapshot.Topology.ClusterView.Version
	curShardViews := snapshot.Topology.ShardViewsMapping

	relatedVersionInfo := p.RelatedVersionInfo()
	if relatedVersionInfo.ClusterVersion != curClusterVersion {
		return false
	}
	for shardID, version := range relatedVersionInfo.ShardWithVersion {
		shardView, exists := curShardViews[shardID]
		if !exists {
			return false
		}
		if shardView.Version != version {
			return false
		}
	}
	return true
}

// Promote a waiting procedure to be a running procedure.
// One procedure may be related with multiple shards.
func (m *ManagerImpl) promoteProcedure(_ context.Context) ([]Procedure, error) {
	// Get waiting procedures, it has been sorted in queue.
	queue := m.waitingProcedures

	var readyProcs []Procedure
	// Find next valid procedure.
	for {
		p := queue.Pop()
		if p == nil {
			return readyProcs, nil
		}

		if !checkValid(p, m.metadata) {
			// This procedure is invalid, just remove it.
			continue
		}

		// Try to get shard locks.
		shardIDs := make([]uint64, 0, len(p.RelatedVersionInfo().ShardWithVersion))
		for shardID := range p.RelatedVersionInfo().ShardWithVersion {
			shardIDs = append(shardIDs, uint64(shardID))
		}
		lockResult := m.procedureShardLock.TryLock(shardIDs)
		if lockResult {
			// Get lock success, procedure will be executed.
			readyProcs = append(readyProcs, p)
		} else {
			// Get lock failed, procedure will be put back into the queue.
			if err := queue.Push(p, defaultWaitingQueueDelay); err != nil {
				return nil, err
			}
		}
	}
}
