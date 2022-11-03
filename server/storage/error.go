// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrEncode = coderr.NewCodeError(coderr.Internal, "storage encode")
	ErrDecode = coderr.NewCodeError(coderr.Internal, "storage decode")

	ErrCreateSchemaAgain         = coderr.NewCodeError(coderr.Internal, "storage create schemas")
	ErrCreateClusterAgain        = coderr.NewCodeError(coderr.Internal, "storage create cluster")
	ErrCreateClusterViewAgain    = coderr.NewCodeError(coderr.Internal, "storage create cluster view")
	ErrUpdateClusterViewConflict = coderr.NewCodeError(coderr.Internal, "storage update cluster view")
	ErrCreateTableAgain          = coderr.NewCodeError(coderr.Internal, "storage create tables")
	ErrDeleteTableAgain          = coderr.NewCodeError(coderr.Internal, "storage delete table")
	ErrCreateShardViewAgain      = coderr.NewCodeError(coderr.Internal, "storage create shard view")
	ErrUpdateShardViewConflict   = coderr.NewCodeError(coderr.Internal, "storage update shard view")
)
