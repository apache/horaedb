// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import "context"

type State string

const (
	StateRunning   = "running"
	StateFinished  = "finished"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
)

type Typ = uint

const (
	switchLeader Typ = iota
	mergeShard
)

// Procedure is used to describe how to execute a set of operations from the scheduler, e.g. SwitchLeaderProcedure, MergeShardProcedure.
type Procedure interface {
	// ID of the procedure.
	ID() uint64

	// Type of the procedure.
	Type() Typ

	// Start the procedure.
	Start(ctx context.Context) error

	// Cancel the procedure.
	Cancel(ctx context.Context) error

	// State of the procedure. Retrieve the state of this procedure.
	State() State
}

// nolint
type Manager struct {
	storage    *Storage
	procedures []Procedure
}
