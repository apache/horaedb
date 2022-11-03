// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrCreateCluster            = coderr.NewCodeError(coderr.BadRequest, "create cluster")
	ErrClusterAlreadyExists     = coderr.NewCodeError(coderr.ClusterAlreadyExists, "cluster already exists")
	ErrClusterStateInvalid      = coderr.NewCodeError(coderr.Internal, "cluster state invalid")
	ErrClusterNotFound          = coderr.NewCodeError(coderr.NotFound, "cluster not found")
	ErrClusterViewNotFound      = coderr.NewCodeError(coderr.NotFound, "cluster view not found")
	ErrSchemaNotFound           = coderr.NewCodeError(coderr.NotFound, "schema not found")
	ErrTableNotFound            = coderr.NewCodeError(coderr.NotFound, "table not found")
	ErrShardNotFound            = coderr.NewCodeError(coderr.NotFound, "shard not found")
	ErrNodeNotFound             = coderr.NewCodeError(coderr.NotFound, "node not found")
	ErrNodeIsEmpty              = coderr.NewCodeError(coderr.NotFound, "cluster nodes list is empty")
	ErrShardListIsEmpty         = coderr.NewCodeError(coderr.NotFound, "cluster shard list is empty")
	ErrNodeShardsIsEmpty        = coderr.NewCodeError(coderr.Internal, "node's shard list is empty")
	ErrGetShardView             = coderr.NewCodeError(coderr.Internal, "get shard view")
	ErrTableAlreadyExists       = coderr.NewCodeError(coderr.Internal, "table already exists")
	ErrShardViewVersionNotMatch = coderr.NewCodeError(coderr.Internal, "shard view version not match")
)
