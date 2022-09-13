// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrEncodeCluster         = coderr.NewCodeError(coderr.Internal, "meta storage unmarshal cluster")
	ErrDecodeCluster         = coderr.NewCodeError(coderr.Internal, "meta storage marshal cluster")
	ErrEncodeClusterTopology = coderr.NewCodeError(coderr.Internal, "meta storage unmarshal cluster topology")
	ErrDecodeClusterTopology = coderr.NewCodeError(coderr.Internal, "meta storage marshal cluster topology")
	ErrEncodeShardTopology   = coderr.NewCodeError(coderr.Internal, "meta storage unmarshal shard topology")
	ErrDecodeShardTopology   = coderr.NewCodeError(coderr.Internal, "meta storage marshal shard topology")
	ErrEncodeSchema          = coderr.NewCodeError(coderr.Internal, "meta storage unmarshal schema")
	ErrDecodeSchema          = coderr.NewCodeError(coderr.Internal, "meta storage marshal schema")
	ErrEncodeTable           = coderr.NewCodeError(coderr.Internal, "meta storage unmarshal table")
	ErrDecodeTable           = coderr.NewCodeError(coderr.Internal, "meta storage marshal table")
	ErrEncodeNode            = coderr.NewCodeError(coderr.Internal, "meta storage unmarshal node")
	ErrDecodeNode            = coderr.NewCodeError(coderr.Internal, "meta storage marshal node")

	ErrCreateSchemaAgain          = coderr.NewCodeError(coderr.Internal, "meta storage create schemas")
	ErrCreateClusterAgain         = coderr.NewCodeError(coderr.Internal, "meta storage create cluster")
	ErrCreateClusterTopologyAgain = coderr.NewCodeError(coderr.Internal, "meta storage create cluster topology")
	ErrPutClusterTopologyConflict = coderr.NewCodeError(coderr.Internal, "meta storage put cluster topology")
	ErrCreateTableAgain           = coderr.NewCodeError(coderr.Internal, "meta storage create tables")
	ErrDeleteTableAgain           = coderr.NewCodeError(coderr.Internal, "meta storage delete table")
	ErrCreateShardTopologyAgain   = coderr.NewCodeError(coderr.Internal, "meta storage create shard topology")
	ErrPutShardTopologyConflict   = coderr.NewCodeError(coderr.Internal, "meta storage put shard topology")
)
