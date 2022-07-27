// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrCreateCluster           = coderr.NewCodeError(coderr.BadRequest, "create clusters")
	ErrClusterAlreadyExists    = coderr.NewCodeError(coderr.Internal, "clusters already exists")
	ErrClusterNotFound         = coderr.NewCodeError(coderr.NotFound, "clusters not found")
	ErrClusterTopologyNotFound = coderr.NewCodeError(coderr.NotFound, "clusters not found")
	ErrSchemaNotFound          = coderr.NewCodeError(coderr.NotFound, "schemasCache not found")
	ErrTableNotFound           = coderr.NewCodeError(coderr.NotFound, "table not found")
	ErrShardNotFound           = coderr.NewCodeError(coderr.NotFound, "shard not found")
	ErrNodeNotFound            = coderr.NewCodeError(coderr.NotFound, "node not found")
)
