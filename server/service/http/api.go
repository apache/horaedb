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
	"github.com/CeresDB/ceresmeta/server/limiter"
	"github.com/CeresDB/ceresmeta/server/member"
	"github.com/CeresDB/ceresmeta/server/status"
	"github.com/CeresDB/ceresmeta/server/storage"
	"go.uber.org/zap"
)

const (
	statusSuccess string = "success"
	statusError   string = "error"

	apiPrefix string = "/api/v1"
)

type API struct {
	clusterManager cluster.Manager

	serverStatus *status.ServerStatus

	forwardClient *ForwardClient
	flowLimiter   *limiter.FlowLimiter
}

func NewAPI(clusterManager cluster.Manager, serverStatus *status.ServerStatus, forwardClient *ForwardClient, flowLimiter *limiter.FlowLimiter) *API {
	return &API{
		clusterManager: clusterManager,
		serverStatus:   serverStatus,
		forwardClient:  forwardClient,
		flowLimiter:    flowLimiter,
	}
}

func (a *API) NewAPIRouter() *Router {
	router := New().WithPrefix(apiPrefix).WithInstrumentation(printRequestInsmt)

	// Register API.
	router.Get("/leader", a.getLeaderAddr)
	router.Post("/getShardTables", a.getShardTables)
	router.Post("/transferLeader", a.transferLeader)
	router.Post("/split", a.split)
	router.Post("/route", a.route)
	router.Post("/dropTable", a.dropTable)
	router.Post("/getNodeShards", a.getNodeShards)
	router.Get("/flowLimiter", a.getFlowLimiter)
	router.Put("/flowLimiter", a.updateFlowLimiter)
	router.Get("/healthCheck", a.healthCheck)

	// Register cluster API.
	router.Get("/clusters", a.listClusters)
	router.Post("/clusters", a.createCluster)
	router.Put("/clusters/:name", a.updateCluster)

	// Register pprof API.
	router.Get("/debug/pprof/profile", pprof.Profile)
	router.Get("/debug/pprof/symbol", pprof.Symbol)
	router.Get("/debug/pprof/trace", pprof.Trace)
	router.Get("/debug/pprof/heap", a.pprofHeap)
	router.Get("/debug/pprof/allocs", a.pprofAllocs)
	router.Get("/debug/pprof/block", a.pprofBlock)
	router.Get("/debug/pprof/goroutine", a.pprofGoroutine)
	router.Get("/debug/pprof/threadCreate", a.pprofThreadcreate)

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

func (a *API) respondForward(w http.ResponseWriter, response *http.Response) {
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

func (a *API) respond(w http.ResponseWriter, data interface{}) {
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

func (a *API) respondError(w http.ResponseWriter, apiErr coderr.CodeError, msg string) {
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

func (a *API) getLeaderAddr(writer http.ResponseWriter, req *http.Request) {
	leaderAddr, err := a.forwardClient.GetLeaderAddr(req.Context())
	if err != nil {
		log.Error("get leader addr failed", zap.Error(err))
		a.respondError(writer, member.ErrGetLeader, fmt.Sprintf("err: %s", err.Error()))
		return
	}
	a.respond(writer, leaderAddr)
}

type GetShardTablesRequest struct {
	ClusterName string   `json:"clusterName"`
	ShardIDs    []uint32 `json:"shardIDs"`
}

func (a *API) getShardTables(writer http.ResponseWriter, req *http.Request) {
	resp, isLeader, err := a.forwardClient.forwardToLeader(req)
	if err != nil {
		log.Error("forward to leader failed", zap.Error(err))
		a.respondError(writer, ErrForwardToLeader, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if !isLeader {
		a.respondForward(writer, resp)
		return
	}

	var getShardTablesReq GetShardTablesRequest
	err = json.NewDecoder(req.Body).Decode(&getShardTablesReq)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}
	log.Info("get shard tables request", zap.String("request", fmt.Sprintf("%+v", getShardTablesReq)))

	c, err := a.clusterManager.GetCluster(req.Context(), getShardTablesReq.ClusterName)
	if err != nil {
		log.Error("get cluster failed", zap.String("clusterName", getShardTablesReq.ClusterName), zap.Error(err))
		a.respondError(writer, ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", getShardTablesReq.ClusterName, err.Error()))
		return
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
	a.respond(writer, shardTables)
}

type TransferLeaderRequest struct {
	ClusterName       string `json:"clusterName"`
	ShardID           uint32 `json:"shardID"`
	OldLeaderNodeName string `json:"OldLeaderNodeName"`
	NewLeaderNodeName string `json:"newLeaderNodeName"`
}

func (a *API) transferLeader(writer http.ResponseWriter, req *http.Request) {
	resp, isLeader, err := a.forwardClient.forwardToLeader(req)
	if err != nil {
		log.Error("forward to leader failed", zap.Error(err))
		a.respondError(writer, ErrForwardToLeader, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if !isLeader {
		a.respondForward(writer, resp)
		return
	}

	var transferLeaderRequest TransferLeaderRequest
	err = json.NewDecoder(req.Body).Decode(&transferLeaderRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}
	log.Info("transfer leader request", zap.String("request", fmt.Sprintf("%+v", transferLeaderRequest)))

	c, err := a.clusterManager.GetCluster(req.Context(), transferLeaderRequest.ClusterName)
	if err != nil {
		log.Error("get cluster failed", zap.String("clusterName", transferLeaderRequest.ClusterName), zap.Error(err))
		a.respondError(writer, ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", transferLeaderRequest.ClusterName, err.Error()))
		return
	}

	transferLeaderProcedure, err := c.GetProcedureFactory().CreateTransferLeaderProcedure(req.Context(), coordinator.TransferLeaderRequest{
		Snapshot:          c.GetMetadata().GetClusterSnapshot(),
		ShardID:           storage.ShardID(transferLeaderRequest.ShardID),
		OldLeaderNodeName: transferLeaderRequest.OldLeaderNodeName,
		NewLeaderNodeName: transferLeaderRequest.NewLeaderNodeName,
	})
	if err != nil {
		log.Error("create transfer leader procedure failed", zap.Error(err))
		a.respondError(writer, ErrCreateProcedure, fmt.Sprintf("err: %s", err.Error()))
		return
	}
	err = c.GetProcedureManager().Submit(req.Context(), transferLeaderProcedure)
	if err != nil {
		log.Error("submit transfer leader procedure failed", zap.Error(err))
		a.respondError(writer, ErrSubmitProcedure, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	a.respond(writer, nil)
}

type RouteRequest struct {
	ClusterName string   `json:"clusterName"`
	SchemaName  string   `json:"schemaName"`
	Tables      []string `json:"table"`
}

func (a *API) route(writer http.ResponseWriter, req *http.Request) {
	resp, isLeader, err := a.forwardClient.forwardToLeader(req)
	if err != nil {
		log.Error("forward to leader failed", zap.Error(err))
		a.respondError(writer, ErrForwardToLeader, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if !isLeader {
		a.respondForward(writer, resp)
		return
	}

	var routeRequest RouteRequest
	err = json.NewDecoder(req.Body).Decode(&routeRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}
	log.Info("route request", zap.String("request", fmt.Sprintf("%+v", routeRequest)))

	result, err := a.clusterManager.RouteTables(context.Background(), routeRequest.ClusterName, routeRequest.SchemaName, routeRequest.Tables)
	if err != nil {
		log.Error("route tables failed", zap.Error(err))
		a.respondError(writer, ErrRouteTable, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	a.respond(writer, result)
}

type NodeShardsRequest struct {
	ClusterName string `json:"clusterName"`
}

func (a *API) getNodeShards(writer http.ResponseWriter, req *http.Request) {
	resp, isLeader, err := a.forwardClient.forwardToLeader(req)
	if err != nil {
		log.Error("forward to leader failed", zap.Error(err))
		a.respondError(writer, ErrForwardToLeader, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if !isLeader {
		a.respondForward(writer, resp)
		return
	}
	var nodeShardsRequest NodeShardsRequest
	err = json.NewDecoder(req.Body).Decode(&nodeShardsRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	result, err := a.clusterManager.GetNodeShards(context.Background(), nodeShardsRequest.ClusterName)
	if err != nil {
		log.Error("get node shards failed", zap.Error(err))
		a.respondError(writer, ErrGetNodeShards, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	a.respond(writer, result)
}

type DropTableRequest struct {
	ClusterName string `json:"clusterName"`
	SchemaName  string `json:"schemaName"`
	Table       string `json:"table"`
}

func (a *API) dropTable(writer http.ResponseWriter, req *http.Request) {
	resp, isLeader, err := a.forwardClient.forwardToLeader(req)
	if err != nil {
		log.Error("forward to leader failed", zap.Error(err))
		a.respondError(writer, ErrForwardToLeader, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if !isLeader {
		a.respondForward(writer, resp)
		return
	}

	var dropTableRequest DropTableRequest
	err = json.NewDecoder(req.Body).Decode(&dropTableRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}
	log.Info("drop table reqeust", zap.String("request", fmt.Sprintf("%+v", dropTableRequest)))

	if err := a.clusterManager.DropTable(context.Background(), dropTableRequest.ClusterName, dropTableRequest.SchemaName, dropTableRequest.Table); err != nil {
		log.Error("drop table failed", zap.Error(err))
		a.respondError(writer, ErrDropTable, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	a.respond(writer, nil)
}

type SplitRequest struct {
	ClusterName string   `json:"clusterName"`
	SchemaName  string   `json:"schemaName"`
	ShardID     uint32   `json:"shardID"`
	SplitTables []string `json:"splitTables"`
	NodeName    string   `json:"nodeName"`
}

func (a *API) split(writer http.ResponseWriter, req *http.Request) {
	resp, isLeader, err := a.forwardClient.forwardToLeader(req)
	if err != nil {
		log.Error("forward to leader failed", zap.Error(err))
		a.respondError(writer, ErrForwardToLeader, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if !isLeader {
		a.respondForward(writer, resp)
		return
	}

	var splitRequest SplitRequest
	err = json.NewDecoder(req.Body).Decode(&splitRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}
	ctx := context.Background()

	c, err := a.clusterManager.GetCluster(ctx, splitRequest.ClusterName)
	if err != nil {
		log.Error("get cluster failed", zap.String("clusterName", splitRequest.ClusterName), zap.Error(err))
		a.respondError(writer, ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", splitRequest.ClusterName, err.Error()))
		return
	}

	newShardID, err := c.GetMetadata().AllocShardID(ctx)
	if err != nil {
		log.Error("alloc shard id failed", zap.Error(err))
		a.respondError(writer, ErrAllocShardID, fmt.Sprintf("err: %s", err.Error()))
		return
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
		a.respondError(writer, ErrCreateProcedure, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if err := c.GetProcedureManager().Submit(ctx, splitProcedure); err != nil {
		log.Error("submit split procedure failed", zap.Error(err))
		a.respondError(writer, ErrSubmitProcedure, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	a.respond(writer, newShardID)
}

func (a *API) listClusters(writer http.ResponseWriter, req *http.Request) {
	resp, isLeader, err := a.forwardClient.forwardToLeader(req)
	if err != nil {
		log.Error("forward to leader failed", zap.Error(err))
		a.respondError(writer, ErrForwardToLeader, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if !isLeader {
		a.respondForward(writer, resp)
		return
	}

	clusters, err := a.clusterManager.ListClusters(req.Context())
	if err != nil {
		log.Error("list clusters failed", zap.Error(err))
		a.respondError(writer, ErrGetCluster, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	clusterMetadatas := make([]storage.Cluster, 0, len(clusters))
	for i := 0; i < len(clusters); i++ {
		storageMetadata := clusters[i].GetMetadata().GetStorageMetadata()
		clusterMetadatas = append(clusterMetadatas, storageMetadata)
	}
	a.respond(writer, clusterMetadatas)
}

type CreateClusterRequest struct {
	Name           string `json:"Name"`
	NodeCount      uint32 `json:"NodeCount"`
	ShardTotal     uint32 `json:"ShardTotal"`
	EnableSchedule bool   `json:"enableSchedule"`
	TopologyType   string `json:"topologyType"`
}

func (a *API) createCluster(writer http.ResponseWriter, req *http.Request) {
	resp, isLeader, err := a.forwardClient.forwardToLeader(req)
	if err != nil {
		log.Error("forward to leader failed", zap.Error(err))
		a.respondError(writer, ErrForwardToLeader, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if !isLeader {
		a.respondForward(writer, resp)
		return
	}

	var createClusterRequest CreateClusterRequest
	err = json.NewDecoder(req.Body).Decode(&createClusterRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if _, err := a.clusterManager.GetCluster(req.Context(), createClusterRequest.Name); err == nil {
		log.Error("cluster already exists", zap.String("clusterName", createClusterRequest.Name))
		a.respondError(writer, ErrGetCluster, fmt.Sprintf("cluster: %s already exists", createClusterRequest.Name))
		return
	}

	topologyType, err := metadata.ParseTopologyType(createClusterRequest.TopologyType)
	if err != nil {
		log.Error("parse topology type failed", zap.Error(err))
		a.respondError(writer, ErrParseTopology, fmt.Sprintf("err: %s", err.Error()))
		return
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
		a.respondError(writer, metadata.ErrCreateCluster, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	a.respond(writer, c.GetMetadata().GetClusterID())
}

type UpdateClusterRequest struct {
	NodeCount      uint32 `json:"NodeCount"`
	ShardTotal     uint32 `json:"ShardTotal"`
	EnableSchedule bool   `json:"enableSchedule"`
	TopologyType   string `json:"topologyType"`
}

func (a *API) updateCluster(writer http.ResponseWriter, req *http.Request) {
	resp, isLeader, err := a.forwardClient.forwardToLeader(req)
	if err != nil {
		log.Error("forward to leader failed", zap.Error(err))
		a.respondError(writer, ErrForwardToLeader, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if !isLeader {
		a.respondForward(writer, resp)
		return
	}

	clusterName := Param(req.Context(), "name")
	if len(clusterName) == 0 {
		a.respondError(writer, ErrParseRequest, "clusterName cloud not be empty")
		return
	}

	var updateClusterRequest UpdateClusterRequest
	err = json.NewDecoder(req.Body).Decode(&updateClusterRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	c, err := a.clusterManager.GetCluster(req.Context(), clusterName)
	if err != nil {
		log.Error("get cluster failed", zap.Error(err))
		a.respondError(writer, ErrGetCluster, fmt.Sprintf("clusterName: %s, err: %s", clusterName, err.Error()))
		return
	}

	topologyType, err := metadata.ParseTopologyType(updateClusterRequest.TopologyType)
	if err != nil {
		log.Error("parse topology type", zap.Error(err))
		a.respondError(writer, ErrParseTopology, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if err := a.clusterManager.UpdateCluster(req.Context(), clusterName, metadata.UpdateClusterOpts{
		EnableSchedule: updateClusterRequest.EnableSchedule,
		TopologyType:   topologyType,
	}); err != nil {
		log.Error("update cluster failed", zap.Error(err))
		a.respondError(writer, metadata.ErrUpdateCluster, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	a.respond(writer, c.GetMetadata().GetClusterID())
}

func (a *API) getFlowLimiter(writer http.ResponseWriter, req *http.Request) {
	resp, isLeader, err := a.forwardClient.forwardToLeader(req)
	if err != nil {
		log.Error("forward to leader failed", zap.Error(err))
		a.respondError(writer, ErrForwardToLeader, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if !isLeader {
		a.respondForward(writer, resp)
		return
	}

	limiter := a.flowLimiter.GetConfig()
	a.respond(writer, limiter)
}

type UpdateFlowLimiterRequest struct {
	Limit  int  `json:"limit"`
	Burst  int  `json:"burst"`
	Enable bool `json:"enable"`
}

func (a *API) updateFlowLimiter(writer http.ResponseWriter, req *http.Request) {
	resp, isLeader, err := a.forwardClient.forwardToLeader(req)
	if err != nil {
		log.Error("forward to leader failed", zap.Error(err))
		a.respondError(writer, ErrForwardToLeader, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if !isLeader {
		a.respondForward(writer, resp)
		return
	}

	var updateFlowLimiterRequest UpdateFlowLimiterRequest
	err = json.NewDecoder(req.Body).Decode(&updateFlowLimiterRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	newLimiterConfig := config.LimiterConfig{
		Limit:  updateFlowLimiterRequest.Limit,
		Burst:  updateFlowLimiterRequest.Burst,
		Enable: updateFlowLimiterRequest.Enable,
	}

	if err := a.flowLimiter.UpdateLimiter(newLimiterConfig); err != nil {
		log.Error("update flow limiter failed", zap.Error(err))
		a.respondError(writer, ErrUpdateFlowLimiter, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	a.respond(writer, nil)
}

func (a *API) healthCheck(writer http.ResponseWriter, _ *http.Request) {
	isServerHealthy := a.serverStatus.IsHealthy()
	if isServerHealthy {
		a.respond(writer, nil)
	} else {
		a.respondError(writer, ErrHealthCheck,
			fmt.Sprintf("server heath check failed, status is %v", a.serverStatus.Get()))
	}
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
