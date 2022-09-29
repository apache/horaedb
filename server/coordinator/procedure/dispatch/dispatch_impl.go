// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package dispatch

import (
	"context"
)

type ActionDispatchImpl struct{}

func NewEventDispatchImpl() *ActionDispatchImpl {
	return &ActionDispatchImpl{}
}

func (d *ActionDispatchImpl) OpenShards(_ context.Context, _ string, _ OpenShardAction) error {
	// TODO: impl later
	return nil
}

func (d *ActionDispatchImpl) CloseShards(_ context.Context, _ string, _ CloseShardAction) error {
	// TODO: impl later
	return nil
}

func (d *ActionDispatchImpl) ChangeShardRole(_ context.Context, _ string, _ ChangeShardRoleAction) error {
	// TODO: impl later
	return nil
}
