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
	"github.com/CeresDB/ceresmeta/server/limiter"
	"github.com/CeresDB/ceresmeta/server/member"
	"github.com/CeresDB/ceresmeta/server/status"
	"github.com/CeresDB/ceresmeta/server/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	statusSuccess    string = "success"
	statusError      string = "error"
	clusterNameParam string = "cluster"

	apiPrefix string = "/api/v1"
)

type apiFuncResult struct {
	data   interface{}
	err    coderr.CodeError
	errMsg string
}

func okResult(data interface{}) apiFuncResult {
	return apiFuncResult{
		data:   data,
		err:    nil,
		errMsg: "",
	}
}

func errResult(err coderr.CodeError, errMsg string) apiFuncResult {
	return apiFuncResult{
		data:   nil,
		err:    err,
		errMsg: errMsg,
	}
}

type apiFunc func(r *http.Request) apiFuncResult

type API struct {
	clusterManager cluster.Manager

	serverStatus *status.ServerStatus

	forwardClient *ForwardClient
	flowLimiter   *limiter.FlowLimiter

	etcdAPI EtcdAPI
}

type DiagnoseShardStatus struct {
	NodeName string `json:"node_name"`
	Status   string `json:"status"`
}

type DiagnoseShardResult struct {
	// shardID -> nodeName
	UnregisteredShards map[storage.ShardID]string              `json:"unregistered_shards"`
	UnreadyShards      map[storage.ShardID]DiagnoseShardStatus `json:"unready_shards"`
}

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
	router.Get("/leader", wrap(a.getLeaderAddr, true, a.forwardClient))
	router.Post("/getShardTables", wrap(a.getShardTables, true, a.forwardClient))
	router.Post("/transferLeader", wrap(a.transferLeader, true, a.forwardClient))
	router.Post("/split", wrap(a.split, true, a.forwardClient))
	router.Post("/route", wrap(a.route, true, a.forwardClient))
	router.Post("/dropTable", wrap(a.dropTable, true, a.forwardClient))
	router.Post("/getNodeShards", wrap(a.getNodeShards, true, a.forwardClient))
	router.Get("/flowLimiter", wrap(a.getFlowLimiter, true, a.forwardClient))
	router.Put("/flowLimiter", wrap(a.updateFlowLimiter, true, a.forwardClient))
	router.Get("/procedures/:name", wrap(a.listProcedures, true, a.forwardClient))
	router.Get("/healthCheck", wrap(a.healthCheck, false, a.forwardClient))

	// Register cluster API.
	router.Get("/clusters", wrap(a.listClusters, true, a.forwardClient))
	router.Post("/clusters", wrap(a.createCluster, true, a.forwardClient))
	router.Put("/clusters/:name", wrap(a.updateCluster, true, a.forwardClient))

	// Register debug API.
	router.Get("/debug/pprof/profile", pprof.Profile)
	router.Get("/debug/pprof/symbol", pprof.Symbol)
	router.Get("/debug/pprof/trace", pprof.Trace)
	router.Get("/debug/pprof/heap", a.pprofHeap)
	router.Get("/debug/pprof/allocs", a.pprofAllocs)
	router.Get("/debug/pprof/block", a.pprofBlock)
	router.Get("/debug/pprof/goroutine", a.pprofGoroutine)
	router.Get("/debug/pprof/threadCreate", a.pprofThreadcreate)
	router.Get("/debug/diagnose/:"+clusterNameParam+"/shards", wrap(a.diagnoseShards, true, a.forwardClient))

	// Register ETCD API.
	router.Post("/etcd/promoteLearner", wrap(a.etcdAPI.promoteLearner, false, a.forwardClient))
	router.Put("/etcd/member", wrap(a.etcdAPI.addMember, false, a.forwardClient))
	router.Get("/etcd/member", wrap(a.etcdAPI.getMember, false, a.forwardClient))
	router.Post("/etcd/member", wrap(a.etcdAPI.updateMember, false, a.forwardClient))
	router.Del("/etcd/member", wrap(a.etcdAPI.removeMember, false, a.forwardClient))
	router.Post("/etcd/moveLeader", wrap(a.etcdAPI.moveLeader, false, a.forwardClient))

	return router
}

// printRequestInsmt used for printing every request information.
func printRequestInsmt(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		body := ""
		bodyByte, err := io.ReadAll(request.Body)
		if err == nil {
			body = string(bodyByte)
			newBody := io.NopCloser(bytes.NewReader(bodyByte))
			request.Body = newBody
		}
		log.Info("receive http request", zap.String("handlerName", handlerName), zap.String("client host", request.RemoteAddr), zap.String("method", request.Method), zap.String("params", request.Form.Encode()), zap.String("body", body))
		handler.ServeHTTP(writer, request)
	}
}

type response struct {
	Status string      `json:"status"`
	Data   interface{} `json:"data,omitempty"`
	Error  string      `json:"error,omitempty"`
	Msg    string      `json:"msg,omitempty"`
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
				respondError(w, ErrForwardToLeader, fmt.Sprintf("err: %s", err.Error()))
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

func (a *API) getLeaderAddr(req *http.Request) apiFuncResult {
	leaderAddr, err := a.forwardClient.GetLeaderAddr(req.Context())
	if err != nil {
		log.Error("get leader addr failed", zap.Error(err))
		return errResult(member.ErrGetLeader, fmt.Sprintf("err: %s", err.Error()))
	}
	return okResult(leaderAddr)
}

type GetShardTablesRequest struct {
	ClusterName string   `json:"clusterName"`
	ShardIDs    []uint32 `json:"shardIDs"`
}

func (a *API) getShardTables(req *http.Request) apiFuncResult {
	var getShardTablesReq GetShardTablesRequest
	err := json.NewDecoder(req.Body).Decode(&getShardTablesReq)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
	}
	log.Info("get shard tables request", zap.String("request", fmt.Sprintf("%+v", getShardTablesReq)))

	c, err := a.clusterManager.GetCluster(req.Context(), getShardTablesReq.ClusterName)
	if err != nil {
		log.Error("get cluster failed", zap.String("clusterName", getShardTablesReq.ClusterName), zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
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

type TransferLeaderRequest struct {
	ClusterName       string `json:"clusterName"`
	ShardID           uint32 `json:"shardID"`
	OldLeaderNodeName string `json:"OldLeaderNodeName"`
	NewLeaderNodeName string `json:"newLeaderNodeName"`
}

func (a *API) transferLeader(req *http.Request) apiFuncResult {
	var transferLeaderRequest TransferLeaderRequest
	err := json.NewDecoder(req.Body).Decode(&transferLeaderRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
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
		return errResult(ErrCreateProcedure, fmt.Sprintf("err: %s", err.Error()))
	}
	err = c.GetProcedureManager().Submit(req.Context(), transferLeaderProcedure)
	if err != nil {
		log.Error("submit transfer leader procedure failed", zap.Error(err))
		return errResult(ErrSubmitProcedure, fmt.Sprintf("err: %s", err.Error()))
	}

	return okResult("ok")
}

type RouteRequest struct {
	ClusterName string   `json:"clusterName"`
	SchemaName  string   `json:"schemaName"`
	Tables      []string `json:"table"`
}

func (a *API) route(req *http.Request) apiFuncResult {
	var routeRequest RouteRequest
	err := json.NewDecoder(req.Body).Decode(&routeRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
	}
	log.Info("route request", zap.String("request", fmt.Sprintf("%+v", routeRequest)))

	result, err := a.clusterManager.RouteTables(context.Background(), routeRequest.ClusterName, routeRequest.SchemaName, routeRequest.Tables)
	if err != nil {
		log.Error("route tables failed", zap.Error(err))
		return errResult(ErrRouteTable, fmt.Sprintf("err: %s", err.Error()))
	}

	return okResult(result)
}

type NodeShardsRequest struct {
	ClusterName string `json:"clusterName"`
}

func (a *API) getNodeShards(req *http.Request) apiFuncResult {
	var nodeShardsRequest NodeShardsRequest
	err := json.NewDecoder(req.Body).Decode(&nodeShardsRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
	}

	result, err := a.clusterManager.GetNodeShards(context.Background(), nodeShardsRequest.ClusterName)
	if err != nil {
		log.Error("get node shards failed", zap.Error(err))
		return errResult(ErrGetNodeShards, fmt.Sprintf("err: %s", err.Error()))
	}

	return okResult(result)
}

type DropTableRequest struct {
	ClusterName string `json:"clusterName"`
	SchemaName  string `json:"schemaName"`
	Table       string `json:"table"`
}

func (a *API) dropTable(req *http.Request) apiFuncResult {
	var dropTableRequest DropTableRequest
	err := json.NewDecoder(req.Body).Decode(&dropTableRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
	}
	log.Info("drop table reqeust", zap.String("request", fmt.Sprintf("%+v", dropTableRequest)))

	if err := a.clusterManager.DropTable(context.Background(), dropTableRequest.ClusterName, dropTableRequest.SchemaName, dropTableRequest.Table); err != nil {
		log.Error("drop table failed", zap.Error(err))
		return errResult(ErrDropTable, fmt.Sprintf("err: %s", err.Error()))
	}

	return okResult("ok")
}

type SplitRequest struct {
	ClusterName string   `json:"clusterName"`
	SchemaName  string   `json:"schemaName"`
	ShardID     uint32   `json:"shardID"`
	SplitTables []string `json:"splitTables"`
	NodeName    string   `json:"nodeName"`
}

func (a *API) split(req *http.Request) apiFuncResult {
	var splitRequest SplitRequest
	err := json.NewDecoder(req.Body).Decode(&splitRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
	}
	ctx := context.Background()

	c, err := a.clusterManager.GetCluster(ctx, splitRequest.ClusterName)
	if err != nil {
		log.Error("get cluster failed", zap.String("clusterName", splitRequest.ClusterName), zap.Error(err))
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", splitRequest.ClusterName, err.Error()))
	}

	newShardID, err := c.GetMetadata().AllocShardID(ctx)
	if err != nil {
		log.Error("alloc shard id failed", zap.Error(err))
		return errResult(ErrAllocShardID, fmt.Sprintf("err: %s", err.Error()))
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
		return errResult(ErrCreateProcedure, fmt.Sprintf("err: %s", err.Error()))
	}

	if err := c.GetProcedureManager().Submit(ctx, splitProcedure); err != nil {
		log.Error("submit split procedure failed", zap.Error(err))
		return errResult(ErrSubmitProcedure, fmt.Sprintf("err: %s", err.Error()))
	}

	return okResult(newShardID)
}

func (a *API) listClusters(req *http.Request) apiFuncResult {
	clusters, err := a.clusterManager.ListClusters(req.Context())
	if err != nil {
		log.Error("list clusters failed", zap.Error(err))
		return errResult(ErrGetCluster, fmt.Sprintf("err: %s", err.Error()))
	}

	clusterMetadatas := make([]storage.Cluster, 0, len(clusters))
	for i := 0; i < len(clusters); i++ {
		storageMetadata := clusters[i].GetMetadata().GetStorageMetadata()
		clusterMetadatas = append(clusterMetadatas, storageMetadata)
	}

	return okResult(clusterMetadatas)
}

type CreateClusterRequest struct {
	Name           string `json:"Name"`
	NodeCount      uint32 `json:"NodeCount"`
	ShardTotal     uint32 `json:"ShardTotal"`
	EnableSchedule bool   `json:"enableSchedule"`
	TopologyType   string `json:"topologyType"`
}

func (a *API) createCluster(req *http.Request) apiFuncResult {
	var createClusterRequest CreateClusterRequest
	err := json.NewDecoder(req.Body).Decode(&createClusterRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
	}

	if _, err := a.clusterManager.GetCluster(req.Context(), createClusterRequest.Name); err == nil {
		log.Error("cluster already exists", zap.String("clusterName", createClusterRequest.Name))
		return errResult(ErrGetCluster, fmt.Sprintf("cluster: %s already exists", createClusterRequest.Name))
	}

	topologyType, err := metadata.ParseTopologyType(createClusterRequest.TopologyType)
	if err != nil {
		log.Error("parse topology type failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
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
		return errResult(metadata.ErrCreateCluster, fmt.Sprintf("err: %s", err.Error()))
	}

	return okResult(c.GetMetadata().GetClusterID())
}

type UpdateClusterRequest struct {
	NodeCount                   uint32 `json:"nodeCount"`
	ShardTotal                  uint32 `json:"shardTotal"`
	EnableSchedule              bool   `json:"enableSchedule"`
	TopologyType                string `json:"topologyType"`
	ProcedureExecutingBatchSize uint32 `json:"procedureExecutingBatchSize"`
}

func (a *API) updateCluster(req *http.Request) apiFuncResult {
	clusterName := Param(req.Context(), "name")
	if len(clusterName) == 0 {
		return errResult(ErrParseRequest, "clusterName cloud not be empty")
	}

	var updateClusterRequest UpdateClusterRequest
	err := json.NewDecoder(req.Body).Decode(&updateClusterRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
	}

	c, err := a.clusterManager.GetCluster(req.Context(), clusterName)
	if err != nil {
		log.Error("get cluster failed", zap.Error(err))
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", clusterName, err.Error()))
	}

	topologyType, err := metadata.ParseTopologyType(updateClusterRequest.TopologyType)
	if err != nil {
		log.Error("parse topology type", zap.Error(err))
		return errResult(ErrParseTopology, fmt.Sprintf("err: %s", err.Error()))
	}

	if err := a.clusterManager.UpdateCluster(req.Context(), clusterName, metadata.UpdateClusterOpts{
		EnableSchedule:              updateClusterRequest.EnableSchedule,
		TopologyType:                topologyType,
		ProcedureExecutingBatchSize: updateClusterRequest.ProcedureExecutingBatchSize,
	}); err != nil {
		log.Error("update cluster failed", zap.Error(err))
		return errResult(metadata.ErrUpdateCluster, fmt.Sprintf("err: %s", err.Error()))
	}

	return okResult(c.GetMetadata().GetClusterID())
}

func (a *API) getFlowLimiter(_ *http.Request) apiFuncResult {
	limiter := a.flowLimiter.GetConfig()
	return okResult(limiter)
}

type UpdateFlowLimiterRequest struct {
	Limit  int  `json:"limit"`
	Burst  int  `json:"burst"`
	Enable bool `json:"enable"`
}

func (a *API) updateFlowLimiter(req *http.Request) apiFuncResult {
	var updateFlowLimiterRequest UpdateFlowLimiterRequest
	err := json.NewDecoder(req.Body).Decode(&updateFlowLimiterRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
	}

	newLimiterConfig := config.LimiterConfig{
		Limit:  updateFlowLimiterRequest.Limit,
		Burst:  updateFlowLimiterRequest.Burst,
		Enable: updateFlowLimiterRequest.Enable,
	}

	if err := a.flowLimiter.UpdateLimiter(newLimiterConfig); err != nil {
		log.Error("update flow limiter failed", zap.Error(err))
		return errResult(ErrUpdateFlowLimiter, fmt.Sprintf("err: %s", err.Error()))
	}

	return okResult("ok")
}

func (a *API) listProcedures(req *http.Request) apiFuncResult {
	ctx := req.Context()
	clusterName := Param(ctx, "name")
	if len(clusterName) == 0 {
		return errResult(ErrParseRequest, "clusterName cloud not be empty")
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		log.Error("get cluster failed", zap.Error(err))
		return errResult(ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", clusterName, err.Error()))
	}

	infos, err := c.GetProcedureManager().ListRunningProcedure(ctx)
	if err != nil {
		log.Error("list running procedure failed", zap.Error(err))
		return errResult(procedure.ErrListRunningProcedure, fmt.Sprintf("clusterName: %s", clusterName))
	}

	return okResult(infos)
}

func (a *API) diagnoseShards(req *http.Request) apiFuncResult {
	ctx := req.Context()
	clusterName := Param(ctx, clusterNameParam)
	if len(clusterName) == 0 {
		clusterName = config.DefaultClusterName
	}

	c, err := a.clusterManager.GetCluster(ctx, clusterName)
	if err != nil {
		log.Error("get cluster failed", zap.Error(err))
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

func (a *API) healthCheck(_ *http.Request) apiFuncResult {
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
