// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrParseGetSchemas = coderr.NewCodeError(coderr.Internal, "meta storage get schemas")
	ErrParsePutSchemas = coderr.NewCodeError(coderr.Internal, "meta storage put schemas")

	ErrParseGetCluster = coderr.NewCodeError(coderr.Internal, "meta storage get cluster")
	ErrParsePutCluster = coderr.NewCodeError(coderr.Internal, "meta storage put cluster")

	ErrParseGetClusterTopology = coderr.NewCodeError(coderr.Internal, "meta storage get cluster topology")
	ErrParsePutClusterTopology = coderr.NewCodeError(coderr.Internal, "meta storage put cluster topology")

	ErrParseGetTables = coderr.NewCodeError(coderr.Internal, "meta storage get tables")
	ErrParsePutTables = coderr.NewCodeError(coderr.Internal, "meta storage put tables")

	ErrParseGetShardTopology = coderr.NewCodeError(coderr.Internal, "meta storage get shard topology")
	ErrParsePutShardTopology = coderr.NewCodeError(coderr.Internal, "meta storage put shard topology")

	ErrParseGetNodes = coderr.NewCodeError(coderr.Internal, "meta storage get nodes")
	ErrParsePutNodes = coderr.NewCodeError(coderr.Internal, "meta storage put nodes")
)
