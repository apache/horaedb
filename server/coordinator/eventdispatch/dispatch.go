// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package eventdispatch

import (
	"context"

	"github.com/CeresDB/ceresmeta/server/cluster"
)

type Dispatch interface {
	OpenShard(context context.Context, address string, request OpenShardRequest) error
	CloseShard(context context.Context, address string, request CloseShardRequest) error
	CreateTableOnShard(context context.Context, address string, request CreateTableOnShardRequest) error
	DropTableOnShard(context context.Context, address string, request DropTableOnShardRequest) error
}

type OpenShardRequest struct {
	Shard cluster.ShardInfo
}

type CloseShardRequest struct {
	ShardID uint32
}

type UpdateShardInfo struct {
	CurrShardInfo cluster.ShardInfo
	PrevVersion   uint64
}

type CreateTableOnShardRequest struct {
	UpdateShardInfo      UpdateShardInfo
	TableInfo            cluster.TableInfo
	EncodedSchema        []byte
	Engine               string
	CreateIfNotExist     bool
	Options              map[string]string
	EncodedPartitionInfo []byte
}

type DropTableOnShardRequest struct {
	UpdateShardInfo UpdateShardInfo
	TableInfo       cluster.TableInfo
}
