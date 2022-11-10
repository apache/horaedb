// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package http

import (
	"encoding/json"
	"net/http"

	"github.com/CeresDB/ceresmeta/pkg/coderr"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
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
		log.Info("receive http request", zap.String("handlerName", handlerName), zap.String("client host", request.RemoteAddr), zap.String("method", request.Method), zap.String("params", request.Form.Encode()))
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
	ShardID           uint64 `json:"shardID"`
	NewLeaderNodeName string `json:"newLeaderNodeName"`
}

// TODO: impl this function
func (a *API) transferLeader(writer http.ResponseWriter, req *http.Request) {
	var transferLeaderRequest TransferLeaderRequest
	err := json.NewDecoder(req.Body).Decode(&transferLeaderRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		a.respondError(writer, ErrParseRequest, nil)
		return
	}
	a.respond(writer, "ok")
}
