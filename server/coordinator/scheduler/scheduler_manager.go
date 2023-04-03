// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"go.uber.org/zap"
)

const (
	schedulerInterval = time.Second * 5
)

// Manager used to manage schedulers, it will register all schedulers when it starts.
//
// Each registered scheduler will generate procedures if the cluster topology matches the scheduling condition.
type Manager interface {
	ListScheduler() []Scheduler

	Start(ctx context.Context) error

	Stop(ctx context.Context) error

	// Scheduler will be called when received new heartbeat, every scheduler registered in schedulerManager will be called to generate procedures.
	// Scheduler cloud be schedule with fix time interval or heartbeat.
	Scheduler(ctx context.Context, clusterSnapshot cluster.Snapshot) []procedure.Procedure
}

type ManagerImpl struct {
	procedureManager procedure.Manager
	clusterManager   cluster.Manager
	factory          *coordinator.Factory
	nodePicker       coordinator.NodePicker

	// This lock is used to protect the following field.
	lock               sync.RWMutex
	registerSchedulers []Scheduler
	isRunning          bool
	cancel             context.CancelFunc
}

func NewManager(procedureManager procedure.Manager, clusterManager cluster.Manager, factory *coordinator.Factory) Manager {
	return &ManagerImpl{
		procedureManager:   procedureManager,
		clusterManager:     clusterManager,
		registerSchedulers: []Scheduler{},
		isRunning:          false,
		factory:            factory,
		nodePicker:         coordinator.NewRandomNodePicker(),
	}
}

func (m *ManagerImpl) Stop(_ context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isRunning {
		m.cancel()
		m.registerSchedulers = m.registerSchedulers[:0]
		m.isRunning = false
	}

	return nil
}

func (m *ManagerImpl) Start(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isRunning {
		return nil
	}

	m.initRegister()

	clusters, err := m.clusterManager.ListClusters(ctx)
	if err != nil {
		return err
	}
	ctxWithCancel, cancel := context.WithCancel(ctx)
	for _, c := range clusters {
		c := c
		go func() {
			for {
				select {
				case <-ctxWithCancel.Done():
					log.Info("scheduler manager is canceled")
					return
				default:
					time.Sleep(schedulerInterval)
					// Get latest cluster snapshot.
					clusterSnapshot := c.GetClusterSnapshot()
					log.Info("scheduler manager invoke", zap.String("clusterTopology", fmt.Sprintf("%v", clusterSnapshot)))
					procedures := m.Scheduler(ctxWithCancel, clusterSnapshot)
					for _, p := range procedures {
						if err := m.procedureManager.Submit(ctx, p); err != nil {
							log.Error("scheduler submit new procedure", zap.Uint64("ProcedureID", p.ID()), zap.Error(err))
						}
					}
				}
			}
		}()
	}

	m.isRunning = true
	m.cancel = cancel

	return nil
}

// Schedulers should to be initialized and registered here.
func (m *ManagerImpl) initRegister() {
	assignShardScheduler := NewAssignShardScheduler(m.factory, m.nodePicker)
	m.registerScheduler(assignShardScheduler)
}

func (m *ManagerImpl) registerScheduler(scheduler Scheduler) {
	log.Info("register new scheduler", zap.String("schedulerName", reflect.TypeOf(scheduler).String()), zap.Int("totalSchedulerLen", len(m.registerSchedulers)))
	m.registerSchedulers = append(m.registerSchedulers, scheduler)
}

func (m *ManagerImpl) ListScheduler() []Scheduler {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.registerSchedulers
}

func (m *ManagerImpl) Scheduler(ctx context.Context, clusterSnapshot cluster.Snapshot) []procedure.Procedure {
	// TODO: Every scheduler should run in an independent goroutine.
	for _, scheduler := range m.registerSchedulers {
		result, err := scheduler.Schedule(ctx, clusterSnapshot)
		if err != nil {
			log.Info("scheduler new procedure", zap.String("desc", result.Reason))
		}
	}
	return nil
}
