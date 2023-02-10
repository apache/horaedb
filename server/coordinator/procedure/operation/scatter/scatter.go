// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scatter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	eventPrepare = "EventPrepare"
	eventFailed  = "EventFailed"
	eventSuccess = "EventSuccess"

	stateBegin   = "StateBegin"
	stateWaiting = "StateWaiting"
	stateFinish  = "StateFinish"
	stateFailed  = "StateFailed"

	defaultCheckNodeNumTimeInterval = time.Second * 3
)

var (
	scatterEvents = fsm.Events{
		{Name: eventPrepare, Src: []string{stateBegin}, Dst: stateWaiting},
		{Name: eventSuccess, Src: []string{stateWaiting}, Dst: stateFinish},
		{Name: eventFailed, Src: []string{stateWaiting}, Dst: stateFailed},
	}
	scatterCallbacks = fsm.Callbacks{
		eventPrepare: prepareCallback,
		eventFailed:  failedCallback,
		eventSuccess: successCallback,
	}
)

func prepareCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	c := req.cluster
	ctx := req.ctx

	waitForNodesReady(c)

	if c.GetClusterState() == storage.ClusterStateStable {
		return
	}

	registeredNodes := c.GetRegisteredNodes()
	shardTotal := c.GetTotalShardNum()
	minNodeCount := c.GetClusterMinNodeCount()

	shardNodes, err := AllocNodeShards(shardTotal, minNodeCount, registeredNodes, req.shardIDs)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "alloc node shardNodes failed")
		return
	}

	shardViews := make([]cluster.CreateShardView, 0, shardTotal)
	for _, shard := range shardNodes {
		shardViews = append(shardViews, cluster.CreateShardView{
			ShardID: shard.ID,
			Tables:  []storage.TableID{},
		})
	}

	if err := c.UpdateClusterView(ctx, storage.ClusterStateStable, shardNodes); err != nil {
		procedure.CancelEventWithLog(event, err, "update cluster view")
		return
	}

	if err := c.CreateShardViews(ctx, shardViews); err != nil {
		procedure.CancelEventWithLog(event, err, "create shard views")
		return
	}

	for _, shard := range shardNodes {
		openShardRequest := eventdispatch.OpenShardRequest{
			Shard: cluster.ShardInfo{
				ID:      shard.ID,
				Role:    storage.ShardRoleLeader,
				Version: 0,
			},
		}
		if err := req.dispatch.OpenShard(ctx, shard.NodeName, openShardRequest); err != nil {
			procedure.CancelEventWithLog(event, err, "open shard failed")
			return
		}
	}
}

func waitForNodesReady(c *cluster.Cluster) {
	for {
		time.Sleep(defaultCheckNodeNumTimeInterval)

		nodes := c.GetRegisteredNodes()

		currNodeNum := computeOnlineNodeNum(nodes)
		expectNodeNum := c.GetClusterMinNodeCount()
		log.Warn("wait for cluster node register", zap.Uint32("currNodeNum", currNodeNum), zap.Uint32("expectNodeNum", expectNodeNum))

		if currNodeNum < expectNodeNum {
			continue
		}
		break
	}
}

// Compute the total number of the available nodes.
func computeOnlineNodeNum(nodes []cluster.RegisteredNode) uint32 {
	onlineNodeNum := uint32(0)
	for _, node := range nodes {
		if node.IsOnline() {
			onlineNodeNum++
		}
	}
	return onlineNodeNum
}

// AllocNodeShards Allocates shard ids across the registered nodes, and caller should ensure `minNodeCount <= len(allNodes)`.
func AllocNodeShards(shardTotal uint32, minNodeCount uint32, allNodes []cluster.RegisteredNode, shardIDs []storage.ShardID) ([]storage.ShardNode, error) {
	// If the number of registered nodes exceeds the required number of nodes, intercept the first registered nodes.
	if len(allNodes) > int(minNodeCount) {
		allNodes = allNodes[:minNodeCount]
	}

	shards := make([]storage.ShardNode, 0, shardTotal)

	perNodeShardCount := shardTotal / minNodeCount
	if shardTotal%minNodeCount != 0 {
		perNodeShardCount++
	}

	for i := uint32(0); i < minNodeCount; i++ {
		for j := uint32(0); j < perNodeShardCount; j++ {
			if uint32(len(shards)) < shardTotal {
				shardID := shardIDs[len(shards)]
				// TODO: consider nodesCache state
				shards = append(shards, storage.ShardNode{
					ID:        shardID,
					ShardRole: storage.ShardRoleLeader,
					NodeName:  allNodes[i].Node.Name,
				})
			}
		}
	}

	return shards, nil
}

func successCallback(_ *fsm.Event) {
	log.Info("scatter procedure execute finish")
}

func failedCallback(_ *fsm.Event) {
	// TODO: Use RollbackProcedure to rollback transfer failed
}

// callbackRequest is fsm callbacks param.
type callbackRequest struct {
	cluster  *cluster.Cluster
	ctx      context.Context
	dispatch eventdispatch.Dispatch
	shardIDs []storage.ShardID
}

func NewProcedure(dispatch eventdispatch.Dispatch, cluster *cluster.Cluster, id uint64, shardIDs []storage.ShardID) procedure.Procedure {
	scatterProcedureFsm := fsm.NewFSM(
		stateBegin,
		scatterEvents,
		scatterCallbacks,
	)

	return &Procedure{id: id, state: procedure.StateInit, fsm: scatterProcedureFsm, dispatch: dispatch, cluster: cluster, shardIDs: shardIDs}
}

type Procedure struct {
	id       uint64
	fsm      *fsm.FSM
	dispatch eventdispatch.Dispatch
	cluster  *cluster.Cluster
	shardIDs []storage.ShardID

	lock  sync.RWMutex
	state procedure.State
}

func (p *Procedure) ID() uint64 {
	return p.id
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.Scatter
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	scatterCallbackRequest := &callbackRequest{
		cluster:  p.cluster,
		ctx:      ctx,
		dispatch: p.dispatch,
		shardIDs: p.shardIDs,
	}

	if err := p.fsm.Event(eventPrepare, scatterCallbackRequest); err != nil {
		err1 := p.fsm.Event(eventFailed, scatterCallbackRequest)
		p.updateStateWithLock(procedure.StateFailed)
		if err1 != nil {
			err = errors.WithMessagef(err, "scatter procedure start, fail to send eventScatterFailed err:%v", err1)
		}
		return errors.WithMessage(err, "scatter procedure start")
	}

	if err := p.fsm.Event(eventSuccess, scatterCallbackRequest); err != nil {
		return errors.WithMessage(err, "scatter procedure start")
	}

	p.updateStateWithLock(procedure.StateFinished)
	return nil
}

func (p *Procedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(procedure.StateCancelled)
	return nil
}

func (p *Procedure) State() procedure.State {
	return p.state
}

func (p *Procedure) updateStateWithLock(state procedure.State) {
	p.lock.Lock()
	p.state = state
	p.lock.Unlock()
}

// Following function is used for test.
func newTestEtcdStorage(t *testing.T) (storage.Storage, clientv3.KV, etcdutil.CloseFn) {
	_, client, closeSrv := etcdutil.PrepareEtcdServerAndClient(t)
	storage := storage.NewStorageWithEtcdBackend(client, test.TestRootPath, storage.Options{
		MaxScanLimit: 100, MinScanLimit: 10,
	})
	return storage, client, closeSrv
}

func newTestCluster(ctx context.Context, t *testing.T) (cluster.Manager, *cluster.Cluster) {
	re := require.New(t)
	storage, kv, _ := newTestEtcdStorage(t)
	manager, err := cluster.NewManagerImpl(storage, kv, test.TestRootPath, test.DefaultIDAllocatorStep)
	re.NoError(err)

	cluster, err := manager.CreateCluster(ctx, test.ClusterName, cluster.CreateClusterOpts{
		NodeCount:         test.DefaultNodeCount,
		ReplicationFactor: test.DefaultReplicationFactor,
		ShardTotal:        test.DefaultShardTotal,
	})
	re.NoError(err)
	return manager, cluster
}

func newClusterAndRegisterNode(t *testing.T) (cluster.Manager, *cluster.Cluster) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := test.MockDispatch{}
	m, c := newTestCluster(ctx, t)

	totalShardNum := c.GetTotalShardNum()
	shardIDs := make([]storage.ShardID, 0, totalShardNum)
	for i := uint32(0); i < totalShardNum; i++ {
		shardID, err := c.AllocShardID(ctx)
		re.NoError(err)
		shardIDs = append(shardIDs, storage.ShardID(shardID))
	}
	p := NewProcedure(dispatch, c, 1, shardIDs)
	go func() {
		err := p.Start(ctx)
		re.NoError(err)
	}()

	// Cluster is empty, it should be return and do nothing
	err := c.RegisterNode(ctx, cluster.RegisteredNode{
		Node: storage.Node{
			Name: test.NodeName0,
		}, ShardInfos: []cluster.ShardInfo{},
	})
	re.NoError(err)
	re.Equal(storage.ClusterStateEmpty, c.GetClusterState())

	// Register two node, DefaultNodeCount is satisfied, Initialize shard topology
	err = c.RegisterNode(ctx, cluster.RegisteredNode{
		Node: storage.Node{
			Name: test.NodeName1,
		}, ShardInfos: []cluster.ShardInfo{},
	})
	re.NoError(err)
	return m, c
}

// Prepare a test cluster which has scattered shards and created test schema.
// Notice: sleep(5s) will be called in this function.
func Prepare(t *testing.T) (cluster.Manager, *cluster.Cluster) {
	re := require.New(t)
	manager, cluster := newClusterAndRegisterNode(t)
	// Wait for the cluster to be ready.
	time.Sleep(time.Second * 5)
	_, _, err := cluster.GetOrCreateSchema(context.Background(), test.TestSchemaName)
	re.NoError(err)
	return manager, cluster
}
