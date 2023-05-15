// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/watch"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	schedulerInterval   = time.Second * 5
	defaultHashReplicas = 50
)

// Manager used to manage schedulers, it will register all schedulers when it starts.
//
// Each registered scheduler will generate procedures if the cluster topology matches the scheduling condition.
type Manager interface {
	ListScheduler() []Scheduler

	Start(ctx context.Context) error

	Stop(ctx context.Context) error

	UpdateEnableSchedule(ctx context.Context, enableSchedule bool)

	// Scheduler will be called when received new heartbeat, every scheduler registered in schedulerManager will be called to generate procedures.
	// Scheduler cloud be schedule with fix time interval or heartbeat.
	Scheduler(ctx context.Context, clusterSnapshot metadata.Snapshot) []ScheduleResult
}

type ManagerImpl struct {
	logger           *zap.Logger
	procedureManager procedure.Manager
	factory          *coordinator.Factory
	nodePicker       coordinator.NodePicker
	client           *clientv3.Client
	clusterMetadata  *metadata.ClusterMetadata
	rootPath         string

	// This lock is used to protect the following field.
	lock               sync.RWMutex
	registerSchedulers []Scheduler
	shardWatch         watch.ShardWatch
	isRunning          atomic.Bool
	enableSchedule     bool
	topologyType       storage.TopologyType
}

func NewManager(logger *zap.Logger, procedureManager procedure.Manager, factory *coordinator.Factory, clusterMetadata *metadata.ClusterMetadata, client *clientv3.Client, rootPath string, enableSchedule bool, topologyType storage.TopologyType) Manager {
	var shardWatch watch.ShardWatch
	switch topologyType {
	case storage.TopologyTypeDynamic:
		shardWatch = watch.NewEtcdShardWatch(logger, clusterMetadata.Name(), rootPath, client)
		shardWatch.RegisteringEventCallback(&schedulerWatchCallback{c: clusterMetadata})
	case storage.TopologyTypeStatic:
		shardWatch = watch.NewNoopShardWatch()
	}

	return &ManagerImpl{
		procedureManager:   procedureManager,
		registerSchedulers: []Scheduler{},
		factory:            factory,
		nodePicker:         coordinator.NewConsistentHashNodePicker(logger, defaultHashReplicas),
		clusterMetadata:    clusterMetadata,
		client:             client,
		shardWatch:         shardWatch,
		rootPath:           rootPath,
		enableSchedule:     enableSchedule,
		topologyType:       topologyType,
		logger:             logger,
	}
}

func (m *ManagerImpl) Stop(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isRunning.Load() {
		m.registerSchedulers = m.registerSchedulers[:0]
		m.isRunning.Store(false)
		if err := m.shardWatch.Stop(ctx); err != nil {
			return errors.WithMessage(err, "stop shard watch failed")
		}
	}

	return nil
}

func (m *ManagerImpl) Start(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isRunning.Load() {
		return nil
	}

	m.initRegister()

	if err := m.shardWatch.Start(ctx); err != nil {
		return errors.WithMessage(err, "start shard watch failed")
	}

	go func() {
		m.isRunning.Store(true)
		for {
			if !m.isRunning.Load() {
				m.logger.Info("scheduler manager is canceled")
				return
			}

			time.Sleep(schedulerInterval)
			// Get latest cluster snapshot.
			clusterSnapshot := m.clusterMetadata.GetClusterSnapshot()
			m.logger.Debug("scheduler manager invoke", zap.String("clusterSnapshot", fmt.Sprintf("%v", clusterSnapshot)))

			// TODO: Perhaps these codes related to schedulerOperator need to be refactored.
			// If schedulerOperator is turned on, the scheduler will only be scheduled in the non-stable state.
			if !m.enableSchedule && clusterSnapshot.Topology.ClusterView.State == storage.ClusterStateStable {
				continue
			}
			if clusterSnapshot.Topology.IsPrepareFinished() {
				if err := m.clusterMetadata.UpdateClusterView(ctx, storage.ClusterStateStable, clusterSnapshot.Topology.ClusterView.ShardNodes); err != nil {
					m.logger.Error("update cluster view failed", zap.Error(err))
				}
				continue
			}

			results := m.Scheduler(ctx, clusterSnapshot)
			for _, result := range results {
				if result.Procedure != nil {
					m.logger.Info("scheduler submit new procedure", zap.Uint64("ProcedureID", result.Procedure.ID()), zap.String("Reason", result.Reason))
					if err := m.procedureManager.Submit(ctx, result.Procedure); err != nil {
						m.logger.Error("scheduler submit new procedure failed", zap.Uint64("ProcedureID", result.Procedure.ID()), zap.Error(err))
					}
				}
			}
		}
	}()

	return nil
}

type schedulerWatchCallback struct {
	c *metadata.ClusterMetadata
}

func (callback *schedulerWatchCallback) OnShardRegistered(_ context.Context, _ watch.ShardRegisterEvent) error {
	return nil
}

func (callback *schedulerWatchCallback) OnShardExpired(ctx context.Context, event watch.ShardExpireEvent) error {
	oldLeader := event.OldLeaderNode
	shardID := event.ShardID
	return callback.c.DropShardNode(ctx, []storage.ShardNode{
		{
			ID:        shardID,
			ShardRole: storage.ShardRoleLeader,
			NodeName:  oldLeader,
		},
	})
}

// Schedulers should to be initialized and registered here.
func (m *ManagerImpl) initRegister() {
	var schedulers []Scheduler
	switch m.topologyType {
	case storage.TopologyTypeDynamic:
		schedulers = m.createDynamicTopologySchedulers()
	case storage.TopologyTypeStatic:
		schedulers = m.createStaticTopologySchedulers()
	}
	for i := 0; i < len(schedulers); i++ {
		m.registerScheduler(schedulers[i])
	}
}

func (m *ManagerImpl) createStaticTopologySchedulers() []Scheduler {
	staticTopologyShardScheduler := NewStaticTopologyShardScheduler(m.factory, m.nodePicker)
	return []Scheduler{staticTopologyShardScheduler}
}

func (m *ManagerImpl) createDynamicTopologySchedulers() []Scheduler {
	assignShardScheduler := NewAssignShardScheduler(m.factory, m.nodePicker)
	rebalancedShardScheduler := NewRebalancedShardScheduler(m.logger, m.factory, m.nodePicker)
	return []Scheduler{assignShardScheduler, rebalancedShardScheduler}
}

func (m *ManagerImpl) registerScheduler(scheduler Scheduler) {
	m.logger.Info("register new scheduler", zap.String("schedulerName", reflect.TypeOf(scheduler).String()), zap.Int("totalSchedulerLen", len(m.registerSchedulers)))
	m.registerSchedulers = append(m.registerSchedulers, scheduler)
}

func (m *ManagerImpl) ListScheduler() []Scheduler {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.registerSchedulers
}

func (m *ManagerImpl) Scheduler(ctx context.Context, clusterSnapshot metadata.Snapshot) []ScheduleResult {
	// TODO: Every scheduler should run in an independent goroutine.
	results := make([]ScheduleResult, 0, len(m.registerSchedulers))
	for _, scheduler := range m.registerSchedulers {
		result, err := scheduler.Schedule(ctx, clusterSnapshot)
		if err != nil {
			m.logger.Error("scheduler failed", zap.Error(err))
			continue
		}
		results = append(results, result)
	}
	return results
}

func (m *ManagerImpl) UpdateEnableSchedule(_ context.Context, enableSchedule bool) {
	m.lock.Lock()
	m.enableSchedule = enableSchedule
	m.lock.Unlock()

	m.logger.Info("scheduler manager update enableSchedule", zap.Bool("enableSchedule", enableSchedule))
}
