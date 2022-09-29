// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package dispatch

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
)

type ActionDispatch interface {
	OpenShards(context context.Context, address string, action OpenShardAction) error

	CloseShards(context context.Context, address string, action CloseShardAction) error

	ChangeShardRole(context context.Context, address string, action ChangeShardRoleAction) error
}

type OpenShardAction struct {
	ShardIDs []uint32
}

type CloseShardAction struct {
	ShardIDs []uint32
}

type ChangeShardRoleAction struct {
	ShardID uint32
	Role    clusterpb.ShardRole
}
