// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"

	"github.com/CeresDB/ceresmeta/pkg/coderr"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/config"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/scheduler"
	"github.com/CeresDB/ceresmeta/server/limiter"
	"github.com/CeresDB/ceresmeta/server/member"
	"github.com/CeresDB/ceresmeta/server/status"
	"github.com/CeresDB/ceresmeta/server/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func NewAPI(clusterManager cluster.Manager, serverStatus *status.ServerStatus, forwardClient *ForwardClient, flowLimiter *limiter.FlowLimiter, etcdClient *clientv3.Client) *API {
	return &API{
		clusterManager: clusterManager,
		serverStatus:   serverStatus,
		forwardClient:  forwardClient,
		flowLimiter:    flowLimiter,
		etcdAPI:        NewEtcdAPI(etcdClient, forwardClient),
	}
}

func (a *API) NewAPIRouter() *Router {
	router := New().WithPrefix(apiPrefix).WithInstrumentation(printRequestInsmt)

	// Register API.
	router.Post("/getShardTables", wrap(a.getShardTables, true, a.forwardClient))
	router.Post("/transferLeader", wrap(a.transferLeader, true, a.forwardClient))
	router.Post("/split", wrap(a.split, true, a.forwardClient))
	router.Post("/route", wrap(a.route, true, a.forwardClient))
	router.Del("/table", wrap(a.dropTable, true, a.forwardClient))
	router.Post("/getNodeShards", wrap(a.getNodeShards, true, a.forwardClient))
	router.Get("/flowLimiter", wrap(a.getFlowLimiter, true, a.forwardClient))
	router.Put("/flowLimiter", wrap(a.updateFlowLimiter, true, a.forwardClient))
	router.Get("/health", wrap(a.health, false, a.forwardClient))

	// Register cluster API.
	router.Get("/clusters", wrap(a.listClusters, true, a.forwardClient))
	router.Post("/cluster", wrap(a.createCluster, true, a.forwardClient))
	router.Put(fmt.Sprintf("/cluster/:%s", clusterNameParam), wrap(a.updateCluster, true, a.forwardClient))
	router.Get(fmt.Sprintf("/cluster/:%s/procedure", clusterNameParam), wrap(a.listProcedures, true, a.forwardClient))
	router.Get(fmt.Sprintf("/cluster/:%s/shardAffinities", clusterNameParam), wrap(a.listShardAffinities, true, a.forwardClient))
	router.Post(fmt.Sprintf("/cluster/:%s/shardAffinities", clusterNameParam), wrap(a.addShardAffinities, true, a.forwardClient))
	router.Del(fmt.Sprintf("/cluster/:%s/shardAffinities", clusterNameParam), wrap(a.removeShardAffinities, true, a.forwardClient))
	router.Post("/table/query", wrap(a.queryTable, true, a.forwardClient))
	router.Get(fmt.Sprintf("/cluster/:%s/deployMode", clusterNameParam), wrap(a.getDeployMode, true, a.forwardClient))
	router.Put(fmt.Sprintf("/cluster/:%s/deployMode", clusterNameParam), wrap(a.updateDeployMode, true, a.forwardClient))

	// Register debug API.
	router.GetWithoutPrefix("/debug/pprof/profile", pprof.Profile)
	router.GetWithoutPrefix("/debug/pprof/symbol", pprof.Symbol)
	router.GetWithoutPrefix("/debug/pprof/trace", pprof.Trace)
	router.GetWithoutPrefix("/debug/pprof/heap", a.pprofHeap)
	router.GetWithoutPrefix("/debug/pprof/allocs", a.pprofAllocs)
	router.GetWithoutPrefix("/debug/pprof/block", a.pprofBlock)
	router.GetWithoutPrefix("/debug/pprof/goroutine", a.pprofGoroutine)
	router.GetWithoutPrefix("/debug/pprof/threadCreate", a.pprofThreadcreate)
	router.GetWithoutPrefix(fmt.Sprintf("/debug/diagnose/:%s/shards", clusterNameParam), wrap(a.diagnoseShards, true, a.forwardClient))
	router.GetWithoutPrefix("/debug/leader", wrap(a.getLeader, false, a.forwardClient))

	// Register ETCD API.
	router.Post("/etcd/promoteLearner", wrap(a.etcdAPI.promoteLearner, false, a.forwardClient))
	router.Put("/etcd/member", wrap(a.etcdAPI.addMember, false, a.forwardClient))
	router.Get("/etcd/member", wrap(a.etcdAPI.getMember, false, a.forwardClient))
	router.Post("/etcd/member", wrap(a.etcdAPI.updateMember, false, a.forwardClient))
	router.Del("/etcd/member", wrap(a.etcdAPI.removeMember, false, a.forwardClient))
	router.Post("/etcd/moveLeader", wrap(a.etcdAPI.moveLeader, false, a.forwardClient))

	return router
}

func (a *API) getLeader(req *http.Request) apiFuncResult {
	leaderAddr, err := a.forwardClient.GetLeaderAddr(req.Context())
	if err != nil {
		log.Error("get leader addr failed", zap.Error(err))
		return errResult(member.ErrGetLeader, err.Error())
	}
	return okResult(leaderAddr)
}

func (a *API) getShardTables(req *http.Request) apiFuncResult {
	var getShardTablesReq GetShardTablesRequest
	err := json.NewDecoder(req.Body).Decode(&getShardTablesReq)
	if err != nil {
		return errResult(ErrParseRequest, err.Error())
	}

	c, err := a.clusterManager.GetCluster(req.Context(), getShardTablesReq.ClusterName)
	if err != nil {
		return errResult(ErrParseRequest, err.Error())
	}

	// If ShardIDs in the request is empty, query with all shardIDs in the cluster.
	shardIDs := make([]storage.ShardID, len(getShardTablesReq.ShardIDs))
	if len(getShardTablesReq.ShardIDs) != 0 {
		for _, shardID := range getShardTablesReq.ShardIDs {
			shardIDs = append(shardIDs, storage.ShardID(shardID))
		}
	} else {
		shardViewsMapping := c.GetMetadata().GetClusterSnapshot().Topology.ShardViewsMapping
		for shardID := range shardViewsMapping {
			shardIDs = append(shardIDs, shardID)
		}
	}

	shardTables := c.GetMetadata().GetShardTables(shardIDs)
	return okResult(shardTables)
}

func (a *API) transferLeader(req *http.Request) apiFuncResult {
	var transferLeaderRequest TransferLeaderRequest
	err := json.NewDecoder(req.Body).Decode(&transferLeaderRequest)
	if err != nil {
		return errResult(ErrParseRequest, err.Error())
	}
	log.Info("transfer leader request", zap.String("request", fmt.Sprintf("%+v", transferLeaderRequest)))

	c, err := a.clusterManager.GetCluster(req.Context(), transferLeaderRequest.ClusterName)
	if err != nil {
		log.Error("get cluster failed", zap.String("clusterName", transferLeaderRequest.ClusterName), zap.Error(err))
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", transferLeaderRequest.ClusterName, err.Error()))
	}

	transferLeaderProcedure, err := c.GetProcedureFactory().CreateTransferLeaderProcedure(req.Context(), coordinator.TransferLeaderRequest{
		Snapshot:          c.GetMetadata().GetClusterSnapshot(),
		ShardID:           storage.ShardID(transferLeaderRequest.ShardID),
		OldLeaderNodeName: transferLeaderRequest.OldLeaderNodeName,
		NewLeaderNodeName: transferLeaderRequest.NewLeaderNodeName,
	})
	if err != nil {
		log.Error("create transfer leader procedure failed", zap.Error(err))
		return errResult(ErrCreateProcedure, err.Error())
	}
	err = c.GetProcedureManager().Submit(req.Context(), transferLeaderProcedure)
	if err != nil {
		log.Error("submit transfer leader procedure failed", zap.Error(err))
		return errResult(ErrSubmitProcedure, err.Error())
	}

	return okResult(statusSuccess)
}

func (a *API) route(req *http.Request) apiFuncResult {
	var routeRequest RouteRequest
	err := json.NewDecoder(req.Body).Decode(&routeRequest)
	if err != nil {
		return errResult(ErrParseRequest, err.Error())
	}

	result, err := a.clusterManager.RouteTables(context.Background(), routeRequest.ClusterName, routeRequest.SchemaName, routeRequest.Tables)
	if err != nil {
		log.Error("route tables failed", zap.Error(err))
		return errResult(ErrRoute, err.Error())
	}

	return okResult(result)
}

func (a *API) getNodeShards(req *http.Request) apiFuncResult {
	var nodeShardsRequest NodeShardsRequest
	err := json.NewDecoder(req.Body).Decode(&nodeShardsRequest)
	if err != nil {
		return errResult(ErrParseRequest, err.Error())
	}

	result, err := a.clusterManager.GetNodeShards(context.Background(), nodeShardsRequest.ClusterName)
	if err != nil {
		log.Error("get node shards failed", zap.Error(err))
		return errResult(ErrGetNodeShards, err.Error())
	}

	return okResult(result)
}

func (a *API) dropTable(req *http.Request) apiFuncResult {
	var dropTableRequest DropTableRequest
	err := json.NewDecoder(req.Body).Decode(&dropTableRequest)
	if err != nil {
		return errResult(ErrParseRequest, err.Error())
	}
	log.Info("drop table reqeust", zap.String("request", fmt.Sprintf("%+v", dropTableRequest)))

	if err := a.clusterManager.DropTable(context.Background(), dropTableRequest.ClusterName, dropTableRequest.SchemaName, dropTableRequest.Table); err != nil {
		log.Error("drop table failed", zap.Error(err))
		return errResult(ErrTable, err.Error())
	}

	return okResult(statusSuccess)
}

func (a *API) split(req *http.Request) apiFuncResult {
	var splitRequest SplitRequest
	err := json.NewDecoder(req.Body).Decode(&splitRequest)
	if err != nil {
		return errResult(ErrParseRequest, err.Error())
	}

	log.Info("split request", zap.String("request", fmt.Sprintf("%+v", splitRequest)))

	ctx := context.Background()

	c, err := a.clusterManager.GetCluster(ctx, splitRequest.ClusterName)
	if err != nil {
		log.Error("get cluster failed", zap.String("clusterName", splitRequest.ClusterName), zap.Error(err))
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", splitRequest.ClusterName, err.Error()))
	}

	newShardID, err := c.GetMetadata().AllocShardID(ctx)
	if err != nil {
		log.Error("alloc shard id failed", zap.Error(err))
		return errResult(ErrAllocShardID, err.Error())
	}

	splitProcedure, err := c.GetProcedureFactory().CreateSplitProcedure(ctx, coordinator.SplitRequest{
		ClusterMetadata: c.GetMetadata(),
		SchemaName:      splitRequest.SchemaName,
		TableNames:      splitRequest.SplitTables,
		Snapshot:        c.GetMetadata().GetClusterSnapshot(),
		ShardID:         storage.ShardID(splitRequest.ShardID),
		NewShardID:      storage.ShardID(newShardID),
		TargetNodeName:  splitRequest.NodeName,
	})
	if err != nil {
		log.Error("create split procedure failed", zap.Error(err))
		return errResult(ErrCreateProcedure, err.Error())
	}

	if err := c.GetProcedureManager().Submit(ctx, splitProcedure); err != nil {
		log.Error("submit split procedure failed", zap.Error(err))
		return errResult(ErrSubmitProcedure, err.Error())
	}

	return okResult(newShardID)
}

func (a *API) listClusters(req *http.Request) apiFuncResult {
	clusters, err := a.clusterManager.ListClusters(req.Context())
	if err != nil {
		return errResult(ErrGetCluster, err.Error())
	}

	clusterMetadatas := make([]storage.Cluster, 0, len(clusters))
	for i := 0; i < len(clusters); i++ {
		storageMetadata := clusters[i].GetMetadata().GetStorageMetadata()
		clusterMetadatas = append(clusterMetadatas, storageMetadata)
	}

	return okResult(clusterMetadatas)
}

func (a *API) createCluster(req *http.Request) apiFuncResult {
	var createClusterRequest CreateClusterRequest
	err := json.NewDecoder(req.Body).Decode(&createClusterRequest)
	if err != nil {
		return errResult(ErrParseRequest, err.Error())
	}

	log.Info("create cluster request", zap.String("request", fmt.Sprintf("%+v", createClusterRequest)))

	if _, err := a.clusterManager.GetCluster(req.Context(), createClusterRequest.Name); err == nil {
		log.Error("cluster already exists", zap.String("clusterName", createClusterRequest.Name))
		return errResult(ErrGetCluster, fmt.Sprintf("cluster: %s already exists", createClusterRequest.Name))
	}

	topologyType, err := metadata.ParseTopologyType(createClusterRequest.TopologyType)
	if err != nil {
		log.Error("parse topology type failed", zap.Error(err))
		return errResult(ErrParseRequest, err.Error())
	}
	c, err := a.clusterManager.CreateCluster(req.Context(), createClusterRequest.Name, metadata.CreateClusterOpts{
		NodeCount:         createClusterRequest.NodeCount,
		ReplicationFactor: 1,
		ShardTotal:        createClusterRequest.ShardTotal,
		EnableSchedule:    createClusterRequest.EnableSchedule,
		TopologyType:      topologyType,
	})
	if err != nil {
		log.Error("create cluster failed", zap.Error(err))
		return errResult(metadata.ErrCreateCluster, err.Error())
	}

	return okResult(c.GetMetadata().GetClusterID())
}

func (a *API) updateCluster(req *http.Request) apiFuncResult {
	clusterName := Param(req.Context(), clusterNameParam)
	if len(clusterName) == 0 {
		return errResult(ErrParseRequest, "clusterName could not be empty")
	}

	var updateClusterRequest UpdateClusterRequest
	err := json.NewDecoder(req.Body).Decode(&updateClusterRequest)
	if err != nil {
		return errResult(ErrParseRequest, err.Error())
	}

	log.Info("update cluster request", zap.String("request", fmt.Sprintf("%+v", updateClusterRequest)))

	c, err := a.clusterManager.GetCluster(req.Context(), clusterName)
	if err != nil {
		log.Error("get cluster failed", zap.Error(err))
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", clusterName, err.Error()))
	}

	topologyType, err := metadata.ParseTopologyType(updateClusterRequest.TopologyType)
	if err != nil {
		log.Error("parse topology type", zap.Error(err))
		return errResult(ErrParseTopology, err.Error())
	}

	if err := a.clusterManager.UpdateCluster(req.Context(), clusterName, metadata.UpdateClusterOpts{
		EnableSchedule:              updateClusterRequest.EnableSchedule,
		TopologyType:                topologyType,
		ProcedureExecutingBatchSize: updateClusterRequest.ProcedureExecutingBatchSize,
	}); err != nil {
		log.Error("update cluster failed", zap.Error(err))
		return errResult(metadata.ErrUpdateCluster, err.Error())
	}

	return okResult(c.GetMetadata().GetClusterID())
}

func (a *API) getFlowLimiter(_ *http.Request) apiFuncResult {
	limiter := a.flowLimiter.GetConfig()
	return okResult(limiter)
}

func (a *API) updateFlowLimiter(req *http.Request) apiFuncResult {
	var updateFlowLimiterRequest UpdateFlowLimiterRequest
	err := json.NewDecoder(req.Body).Decode(&updateFlowLimiterRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, err.Error())
	}

	log.Info("update flow limiter request", zap.String("request", fmt.Sprintf("%+v", updateFlowLimiterRequest)))

	newLimiterConfig := config.LimiterConfig{
		Limit:  updateFlowLimiterRequest.Limit,
		Burst:  updateFlowLimiterRequest.Burst,
		Enable: updateFlowLimiterRequest.Enable,
	}

	if err := a.flowLimiter.UpdateLimiter(newLimiterConfig); err != nil {
		log.Error("update flow limiter failed", zap.Error(err))
		return errResult(ErrUpdateFlowLimiter, err.Error())
	}

	return okResult(statusSuccess)
}

func (a *API) listProcedures(req *http.Request) apiFuncResult {
	ctx := req.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		return errResult(ErrParseRequest, "clusterName could not be empty")
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", clusterName, err.Error()))
	}

	infos, err := c.GetProcedureManager().ListRunningProcedure(ctx)
	if err != nil {
		log.Error("list running procedure failed", zap.Error(err))
		return errResult(procedure.ErrListRunningProcedure, fmt.Sprintf("clusterName: %s", clusterName))
	}

	return okResult(infos)
}

func (a *API) listShardAffinities(req *http.Request) apiFuncResult {
	ctx := req.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		return errResult(ErrParseRequest, "clusterName could not be empty")
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", clusterName, err.Error()))
	}

	affinityRules, err := c.GetSchedulerManager().ListShardAffinityRules(ctx)
	if err != nil {
		return errResult(ErrListAffinityRules, fmt.Sprintf("err: %v", err))
	}

	return okResult(affinityRules)
}

func (a *API) addShardAffinities(req *http.Request) apiFuncResult {
	ctx := req.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		return errResult(ErrParseRequest, "clusterName could not be empty")
	}

	var affinities []scheduler.ShardAffinity
	err := json.NewDecoder(req.Body).Decode(&affinities)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, err.Error())
	}

	log.Info("try to apply shard affinity rule", zap.String("cluster", clusterName), zap.String("affinity", fmt.Sprintf("%+v", affinities)))

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", clusterName, err.Error()))
	}

	err = c.GetSchedulerManager().AddShardAffinityRule(ctx, scheduler.ShardAffinityRule{Affinities: affinities})
	if err != nil {
		log.Error("failed to apply shard affinity rule", zap.String("cluster", clusterName), zap.String("affinity", fmt.Sprintf("%+v", affinities)))
		return errResult(ErrAddAffinityRule, fmt.Sprintf("err: %v", err))
	}

	log.Info("finish applying shard affinity rule", zap.String("cluster", clusterName), zap.String("rules", fmt.Sprintf("%+v", affinities)))

	return okResult(nil)
}

func (a *API) removeShardAffinities(req *http.Request) apiFuncResult {
	ctx := req.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		return errResult(ErrParseRequest, "clusterName could not be empty")
	}

	var decodedReq RemoveShardAffinitiesRequest
	err := json.NewDecoder(req.Body).Decode(&decodedReq)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, err.Error())
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", clusterName, err.Error()))
	}

	for _, shardID := range decodedReq.ShardIDs {
		log.Info("try to remove shard affinity rule", zap.String("cluster", clusterName), zap.Int("shardID", int(shardID)))
		err := c.GetSchedulerManager().RemoveShardAffinityRule(ctx, shardID)
		if err != nil {
			log.Error("failed to remove shard affinity rule", zap.String("cluster", clusterName), zap.Int("shardID", int(shardID)), zap.Error(err))
			return errResult(ErrRemoveAffinityRule, fmt.Sprintf("err: %s", err))
		}
	}

	return okResult(nil)
}

func (a *API) queryTable(r *http.Request) apiFuncResult {
	var req QueryTableRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return errResult(ErrParseRequest, err.Error())
	}

	if len(req.Names) != 0 {
		tables, err := a.clusterManager.GetTables(req.ClusterName, req.SchemaName, req.Names)
		if err != nil {
			return errResult(ErrTable, err.Error())
		}
		return okResult(tables)
	}

	ids := make([]storage.TableID, 0, len(req.IDs))
	for _, id := range req.IDs {
		ids = append(ids, storage.TableID(id))
	}

	tables, err := a.clusterManager.GetTablesByIDs(req.ClusterName, ids)
	if err != nil {
		return errResult(ErrTable, err.Error())
	}
	return okResult(tables)
}

func (a *API) getDeployMode(r *http.Request) apiFuncResult {
	ctx := r.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		clusterName = config.DefaultClusterName
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", clusterName, err.Error()))
	}

	deployMode, err := c.GetSchedulerManager().GetDeployMode(r.Context())
	if err != nil {
		return errResult(ErrGetDeployMode, err.Error())
	}

	return okResult(deployMode)
}

func (a *API) updateDeployMode(r *http.Request) apiFuncResult {
	ctx := r.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		clusterName = config.DefaultClusterName
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", clusterName, err.Error()))
	}

	var req UpdateDeployModeRequest
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return errResult(ErrParseRequest, err.Error())
	}

	err = c.GetSchedulerManager().UpdateDeployMode(r.Context(), req.Enable)
	if err != nil {
		return errResult(ErrUpdateDeployMode, err.Error())
	}

	return okResult(req.Enable)
}

func (a *API) diagnoseShards(req *http.Request) apiFuncResult {
	ctx := req.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		clusterName = config.DefaultClusterName
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", clusterName, err.Error()))
	}

	registerNodes, err := a.clusterManager.ListRegisterNodes(ctx, clusterName)
	if err != nil {
		log.Error("get cluster failed", zap.Error(err))
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", clusterName, err.Error()))
	}

	expectedShardNodes := c.GetShardNodes()
	// shardName -> shardInfo
	registerNodesMap := make(map[string][]metadata.ShardInfo, len(registerNodes))
	for _, node := range registerNodes {
		registerNodesMap[node.Node.Name] = node.ShardInfos
	}

	ret := DiagnoseShardResult{
		UnregisteredShards: make(map[storage.ShardID]string),
		UnreadyShards:      make(map[storage.ShardID]DiagnoseShardStatus),
	}
	// Check shard not register and not ready.
	for _, shardNode := range expectedShardNodes.ShardNodes {
		shardID := shardNode.ID
		nodeName := shardNode.NodeName
		shardInfo, ok := registerNodesMap[nodeName]
		if !ok {
			ret.UnregisteredShards[shardID] = nodeName
			continue
		}
		for _, info := range shardInfo {
			if info.ID == shardID {
				if info.Status != storage.ShardStatusReady {
					ret.UnreadyShards[shardID] = DiagnoseShardStatus{
						NodeName: nodeName,
						Status:   storage.ConvertShardStatusToString(info.Status),
					}
				}
				break
			}
		}
	}
	return okResult(ret)
}

func (a *API) health(_ *http.Request) apiFuncResult {
	isServerHealthy := a.serverStatus.IsHealthy()
	if isServerHealthy {
		return okResult(nil)
	}
	return errResult(ErrHealthCheck, fmt.Sprintf("server heath check failed, status is %v", a.serverStatus.Get()))
}

func (a *API) pprofHeap(writer http.ResponseWriter, req *http.Request) {
	pprof.Handler("heap").ServeHTTP(writer, req)
}

func (a *API) pprofAllocs(writer http.ResponseWriter, req *http.Request) {
	pprof.Handler("allocs").ServeHTTP(writer, req)
}

func (a *API) pprofBlock(writer http.ResponseWriter, req *http.Request) {
	pprof.Handler("block").ServeHTTP(writer, req)
}

func (a *API) pprofGoroutine(writer http.ResponseWriter, req *http.Request) {
	pprof.Handler("goroutine").ServeHTTP(writer, req)
}

func (a *API) pprofThreadcreate(writer http.ResponseWriter, req *http.Request) {
	pprof.Handler("threadcreate").ServeHTTP(writer, req)
}

// printRequestInsmt used for printing every request information.
func printRequestInsmt(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		body := ""
		bodyByte, err := io.ReadAll(request.Body)
		if err != nil {
			log.Error("read request body failed", zap.Error(err))
			return
		}
		body = string(bodyByte)
		newBody := io.NopCloser(bytes.NewReader(bodyByte))
		request.Body = newBody
		log.Info("receive http request", zap.String("handlerName", handlerName), zap.String("client host", request.RemoteAddr), zap.String("method", request.Method), zap.String("params", request.Form.Encode()), zap.String("body", body))
		handler.ServeHTTP(writer, request)
	}
}

func respondForward(w http.ResponseWriter, response *http.Response) {
	b, err := io.ReadAll(response.Body)
	if err != nil {
		log.Error("read response failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for key, valArr := range response.Header {
		for _, val := range valArr {
			w.Header().Add(key, val)
		}
	}
	w.WriteHeader(response.StatusCode)
	if n, err := w.Write(b); err != nil {
		log.Error("write response failed", zap.Int("msg", n), zap.Error(err))
	}
}

func respond(w http.ResponseWriter, data interface{}) {
	statusMessage := statusSuccess
	b, err := json.Marshal(&response{
		Status: statusMessage,
		Data:   data,
	})
	if err != nil {
		log.Error("marshal json response failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		log.Error("write response failed", zap.Int("msg", n), zap.Error(err))
	}
}

func respondError(w http.ResponseWriter, apiErr coderr.CodeError, msg string) {
	b, err := json.Marshal(&response{
		Status: statusError,
		Error:  apiErr.Error(),
		Msg:    msg,
	})
	if err != nil {
		log.Error("marshal json response failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(apiErr.Code().ToHTTPCode())
	if n, err := w.Write(b); err != nil {
		log.Error("write response failed", zap.Int("msg", n), zap.Error(err))
	}
}

func wrap(f apiFunc, needForward bool, forwardClient *ForwardClient) http.HandlerFunc {
	hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if needForward {
			resp, isLeader, err := forwardClient.forwardToLeader(r)
			if err != nil {
				log.Error("forward to leader failed", zap.Error(err))
				respondError(w, ErrForwardToLeader, err.Error())
				return
			}
			if !isLeader {
				respondForward(w, resp)
				return
			}
		}
		result := f(r)
		if result.err != nil {
			respondError(w, result.err, result.errMsg)
			return
		}
		respond(w, result.data)
	})
	return hf
}
