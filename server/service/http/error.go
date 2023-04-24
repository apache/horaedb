// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package http

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrParseRequest    = coderr.NewCodeError(coderr.BadRequest, "parse request params")
	ErrDropTable       = coderr.NewCodeError(coderr.Internal, "drop table")
	ErrRouteTable      = coderr.NewCodeError(coderr.Internal, "route table")
	ErrGetNodeShards   = coderr.NewCodeError(coderr.Internal, "get node shards")
	ErrCreateProcedure = coderr.NewCodeError(coderr.Internal, "create procedure")
	ErrSubmitProcedure = coderr.NewCodeError(coderr.Internal, "submit procedure")
	ErrGetCluster      = coderr.NewCodeError(coderr.Internal, "get cluster")
	ErrAllocShardID    = coderr.NewCodeError(coderr.Internal, "alloc shard id")
	ErrForwardToLeader = coderr.NewCodeError(coderr.Internal, "forward to leader")
	ErrParseLeaderAddr = coderr.NewCodeError(coderr.Internal, "parse leader addr")
	ErrHealthCheck     = coderr.NewCodeError(coderr.Internal, "server health check fail")
)
