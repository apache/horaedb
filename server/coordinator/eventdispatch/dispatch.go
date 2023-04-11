// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package eventdispatch

import (
	"context"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
)

type Dispatch interface {
	OpenShard(context context.Context, address string, request OpenShardRequest) error
	CloseShard(context context.Context, address string, request CloseShardRequest) error
	CreateTableOnShard(context context.Context, address string, request CreateTableOnShardRequest) error
	DropTableOnShard(context context.Context, address string, request DropTableOnShardRequest) error
	OpenTableOnShard(ctx context.Context, address string, request OpenTableOnShardRequest) error
	CloseTableOnShard(context context.Context, address string, request CloseTableOnShardRequest) error
}

type OpenShardRequest struct {
	Shard metadata.ShardInfo
}

type CloseShardRequest struct {
	ShardID uint32
}

type UpdateShardInfo struct {
	CurrShardInfo metadata.ShardInfo
	PrevVersion   uint64
}

type CreateTableOnShardRequest struct {
	UpdateShardInfo  UpdateShardInfo
	TableInfo        metadata.TableInfo
	EncodedSchema    []byte
	Engine           string
	CreateIfNotExist bool
	Options          map[string]string
}

type DropTableOnShardRequest struct {
	UpdateShardInfo UpdateShardInfo
	TableInfo       metadata.TableInfo
}

type OpenTableOnShardRequest struct {
	UpdateShardInfo UpdateShardInfo
	TableInfo       metadata.TableInfo
}

type CloseTableOnShardRequest struct {
	UpdateShardInfo UpdateShardInfo
	TableInfo       metadata.TableInfo
}
