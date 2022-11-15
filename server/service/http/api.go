// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package http

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/CeresDB/ceresmeta/pkg/coderr"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
)

const (
	statusSuccess string = "success"
	statusError   string = "error"

	apiPrefix string = "/api/v1"
)

type API struct {
	procedureManager procedure.Manager
	procedureFactory *procedure.Factory

	clusterManager cluster.Manager
}

func NewAPI(procedureManager procedure.Manager, procedureFactory *procedure.Factory, clusterManager cluster.Manager) *API {
	return &API{
		procedureManager: procedureManager,
		procedureFactory: procedureFactory,
		clusterManager:   clusterManager,
	}
}

func (a *API) NewAPIRouter() *Router {
	router := New().WithPrefix(apiPrefix).WithInstrumentation(printRequestInsmt)

	router.Post("/transferLeader", a.transferLeader)

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
}

func (a *API) respond(w http.ResponseWriter, data interface{}) {
	statusMessage := statusSuccess
	json := jsoniter.ConfigCompatibleWithStandardLibrary
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

func (a *API) respondError(w http.ResponseWriter, apiErr coderr.CodeError, data interface{}) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status: statusError,
		Error:  apiErr.Error(),
		Data:   data,
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
		a.respondError(writer, ErrParseRequest, nil)
		return
	}

	transferLeaderProcedure, err := a.procedureFactory.CreateTransferLeaderProcedure(req.Context(), procedure.TransferLeaderRequest{
		ClusterName:       transferLeaderRequest.ClusterName,
		ShardID:           storage.ShardID(transferLeaderRequest.ShardID),
		OldLeaderNodeName: transferLeaderRequest.OldLeaderNodeName,
		NewLeaderNodeName: transferLeaderRequest.NewLeaderNodeName,
	})
	if err != nil {
		log.Error("create transfer leader procedure", zap.Error(err))
		a.respondError(writer, procedure.ErrCreateProcedure, nil)
		return
	}
	err = a.procedureManager.Submit(req.Context(), transferLeaderProcedure)
	if err != nil {
		log.Error("submit transfer leader procedure", zap.Error(err))
		a.respondError(writer, procedure.ErrSubmitProcedure, nil)
		return
	}

	a.respond(writer, "ok")
}
