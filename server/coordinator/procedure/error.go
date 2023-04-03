// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrShardLeaderNotFound   = coderr.NewCodeError(coderr.Internal, "shard leader not found")
	ErrProcedureNotFound     = coderr.NewCodeError(coderr.Internal, "procedure not found")
	ErrClusterConfigChanged  = coderr.NewCodeError(coderr.Internal, "cluster config changed")
	ErrTableNotExists        = coderr.NewCodeError(coderr.Internal, "table not exists")
	ErrTableAlreadyExists    = coderr.NewCodeError(coderr.Internal, "table already exists")
	ErrProcedureTypeNotMatch = coderr.NewCodeError(coderr.Internal, "procedure type not match")
	ErrDecodeRawData         = coderr.NewCodeError(coderr.Internal, "decode raw data")
	ErrEncodeRawData         = coderr.NewCodeError(coderr.Internal, "encode raw data")
	ErrGetRequest            = coderr.NewCodeError(coderr.Internal, "get request from event")
	ErrEmptyPartitionNames   = coderr.NewCodeError(coderr.Internal, "partition names is empty")
	ErrDropTableResult       = coderr.NewCodeError(coderr.Internal, "length of shard not correct")
	ErrPickShard             = coderr.NewCodeError(coderr.Internal, "pick shard failed")
)
