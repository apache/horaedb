// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
)

type Manager interface {
	// Start must be called before manager is used.
	Start(ctx context.Context) error
	// Stop must be called before manager is dropped.
	Stop(ctx context.Context) error

	// Submit procedure to be executed asynchronously.
	// TODO: change result type, add channel to get whether the procedure executed successfully
	Submit(ctx context.Context, procedure Procedure) error
	// Cancel procedure that has been submitted.
	Cancel(ctx context.Context, procedureID uint64) error
	ListRunningProcedure(ctx context.Context) ([]*Info, error)
}
