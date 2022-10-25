// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrShardLeaderNotFound  = coderr.NewCodeError(coderr.Internal, "shard leader not found")
	ErrProcedureNotFound    = coderr.NewCodeError(coderr.Internal, "procedure not found")
	ErrClusterConfigChanged = coderr.NewCodeError(coderr.Internal, "cluster config changed")
	ErrTableAlreadyExists   = coderr.NewCodeError(coderr.Internal, "table already exists")
)
