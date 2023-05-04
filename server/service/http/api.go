// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/CeresDB/ceresmeta/pkg/coderr"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
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
}

func NewAPI(clusterManager cluster.Manager, serverStatus *status.ServerStatus, forwardClient *ForwardClient) *API {
	return &API{
		clusterManager: clusterManager,
		serverStatus:   serverStatus,
		forwardClient:  forwardClient,
	}
}

func (a *API) NewAPIRouter() *Router {
	router := New().WithPrefix(apiPrefix).WithInstrumentation(printRequestInsmt)

	// Register post/get API.
	router.Post("/getShardTables", a.getShardTables)
	router.Post("/transferLeader", a.transferLeader)
	router.Post("/split", a.split)
	router.Post("/route", a.route)
	router.Post("/dropTable", a.dropTable)
	router.Post("/getNodeShards", a.getNodeShards)
	router.Put("/enableSchedule", a.enableSchedule)
	router.Get("/healthCheck", a.healthCheck)

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
		log.Error("read resp failed", zap.Error(err))
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
		log.Error("error writing response", zap.Int("msg", n), zap.Error(err))
	}
}

func (a *API) respond(w http.ResponseWriter, data interface{}) {
	statusMessage := statusSuccess
	b, err := json.Marshal(&response{
		Status: statusMessage,
		Data:   data,
	})
	if err != nil {
		log.Error("error marshaling json response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		log.Error("error writing response", zap.Int("msg", n), zap.Error(err))
	}
}

func (a *API) respondError(w http.ResponseWriter, apiErr coderr.CodeError, msg string) {
	b, err := json.Marshal(&response{
		Status: statusError,
		Error:  apiErr.Error(),
		Msg:    msg,
	})
	if err != nil {
		log.Error("error marshaling json response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(apiErr.Code().ToHTTPCode())
	if n, err := w.Write(b); err != nil {
		log.Error("error writing response", zap.Int("msg", n), zap.Error(err))
	}
}

type GetShardTables struct {
	ClusterName string   `json:"clusterName"`
	NodeName    string   `json:"nodeName"`
	ShardIDs    []uint32 `json:"shardIDs"`
}

func (a *API) getShardTables(writer http.ResponseWriter, req *http.Request) {
	var getShardTables GetShardTables
	err := json.NewDecoder(req.Body).Decode(&getShardTables)
	if err != nil {
		a.respondError(writer, ErrParseRequest, "decode request body failed")
		return
	}
	log.Info("get shard tables request", zap.String("request", fmt.Sprintf("%+v", getShardTables)))

	c, err := a.clusterManager.GetCluster(req.Context(), getShardTables.ClusterName)
	if err != nil {
		log.Error("get cluster failed", zap.String("clusterName", getShardTables.ClusterName), zap.Error(err))
		a.respondError(writer, ErrGetCluster, fmt.Sprintf("get cluster failed, clusterName:%s", getShardTables.ClusterName))
		return
	}

	shardIDs := make([]storage.ShardID, len(getShardTables.ShardIDs))
	for _, shardID := range getShardTables.ShardIDs {
		shardIDs = append(shardIDs, storage.ShardID(shardID))
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
	var transferLeaderRequest TransferLeaderRequest
	err := json.NewDecoder(req.Body).Decode(&transferLeaderRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, "decode request body failed")
		return
	}
	log.Info("transfer leader request", zap.String("request", fmt.Sprintf("%+v", transferLeaderRequest)))

	c, err := a.clusterManager.GetCluster(req.Context(), transferLeaderRequest.ClusterName)
	if err != nil {
		log.Error("get cluster failed", zap.String("clusterName", transferLeaderRequest.ClusterName), zap.Error(err))
		a.respondError(writer, ErrGetCluster, fmt.Sprintf("get cluster failed, clusterName:%s", transferLeaderRequest.ClusterName))
		return
	}

	transferLeaderProcedure, err := c.GetProcedureFactory().CreateTransferLeaderProcedure(req.Context(), coordinator.TransferLeaderRequest{
		Snapshot:          c.GetMetadata().GetClusterSnapshot(),
		ShardID:           storage.ShardID(transferLeaderRequest.ShardID),
		OldLeaderNodeName: transferLeaderRequest.OldLeaderNodeName,
		NewLeaderNodeName: transferLeaderRequest.NewLeaderNodeName,
	})
	if err != nil {
		log.Error("create transfer leader procedure", zap.Error(err))
		a.respondError(writer, ErrCreateProcedure, "create transfer leader procedure")
		return
	}
	err = c.GetProcedureManager().Submit(req.Context(), transferLeaderProcedure)
	if err != nil {
		log.Error("submit transfer leader procedure", zap.Error(err))
		a.respondError(writer, ErrSubmitProcedure, "submit transfer leader procedure")
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
		a.respondError(writer, ErrForwardToLeader, "forward to leader failed")
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
		a.respondError(writer, ErrParseRequest, "decode request body failed")
		return
	}
	log.Info("route request", zap.String("request", fmt.Sprintf("%+v", routeRequest)))

	result, err := a.clusterManager.RouteTables(context.Background(), routeRequest.ClusterName, routeRequest.SchemaName, routeRequest.Tables)
	if err != nil {
		log.Error("route tables failed", zap.Error(err))
		a.respondError(writer, ErrRouteTable, "route tables failed")
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
		a.respondError(writer, ErrForwardToLeader, "forward to leader failed")
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
		a.respondError(writer, ErrParseRequest, "decode request body failed")
		return
	}

	result, err := a.clusterManager.GetNodeShards(context.Background(), nodeShardsRequest.ClusterName)
	if err != nil {
		log.Error("get node shards failed", zap.Error(err))
		a.respondError(writer, ErrGetNodeShards, "get node shards failed")
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
	var dropTableRequest DropTableRequest
	err := json.NewDecoder(req.Body).Decode(&dropTableRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, "decode request body failed")
		return
	}
	log.Info("drop table reqeust", zap.String("request", fmt.Sprintf("%+v", dropTableRequest)))

	if err := a.clusterManager.DropTable(context.Background(), dropTableRequest.ClusterName, dropTableRequest.SchemaName, dropTableRequest.Table); err != nil {
		log.Error("cluster drop table failed", zap.Error(err))
		a.respondError(writer, ErrDropTable, "drop table failed")
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
	var splitRequest SplitRequest
	err := json.NewDecoder(req.Body).Decode(&splitRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, "")
		return
	}
	ctx := context.Background()

	c, err := a.clusterManager.GetCluster(ctx, splitRequest.ClusterName)
	if err != nil {
		log.Error("cluster not found", zap.String("clusterName", splitRequest.ClusterName), zap.Error(err))
		a.respondError(writer, metadata.ErrClusterNotFound, "cluster not found")
		return
	}

	newShardID, err := c.GetMetadata().AllocShardID(ctx)
	if err != nil {
		log.Error("alloc shard id failed")
		a.respondError(writer, ErrAllocShardID, "alloc shard id failed")
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
		log.Error("create split procedure", zap.Error(err))
		a.respondError(writer, ErrCreateProcedure, "create split procedure")
		return
	}

	if err := c.GetProcedureManager().Submit(ctx, splitProcedure); err != nil {
		log.Error("submit split procedure", zap.Error(err))
		a.respondError(writer, ErrSubmitProcedure, "submit split procedure")
		return
	}

	a.respond(writer, newShardID)
}

type UpdateEnableScheduleRequest struct {
	ClusterName    string `json:"clusterName"`
	EnableSchedule bool   `json:"enableSchedule"`
}

func (a *API) enableSchedule(writer http.ResponseWriter, req *http.Request) {
	var updateEnableScheduleRequest UpdateEnableScheduleRequest
	err := json.NewDecoder(req.Body).Decode(&updateEnableScheduleRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, "")
		return
	}

	c, err := a.clusterManager.GetCluster(req.Context(), updateEnableScheduleRequest.ClusterName)
	if err != nil {
		log.Error("cluster not found", zap.String("clusterName", updateEnableScheduleRequest.ClusterName), zap.Error(err))
		a.respondError(writer, metadata.ErrClusterNotFound, "cluster not found")
		return
	}

	c.GetSchedulerManager().UpdateEnableSchedule(req.Context(), updateEnableScheduleRequest.EnableSchedule)

	a.respond(writer, nil)
}

func (a *API) healthCheck(writer http.ResponseWriter, _ *http.Request) {
	isServerHealthy := a.serverStatus.IsHealthy()
	if isServerHealthy {
		a.respond(writer, nil)
	} else {
		a.respondError(writer, ErrHealthCheck,
			fmt.Sprintf("server heath check fail, status is %v", a.serverStatus.Get()))
	}
}
