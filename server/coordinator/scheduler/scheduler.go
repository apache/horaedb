// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
)

type ScheduleResult struct {
	Procedure procedure.Procedure
	// The reason that the procedure is generated for.
	Reason string
}

type Scheduler interface {
	// Schedule will generate procedure based on current cluster snapshot, which will be submitted to ProcedureManager, and whether it is actually executed depends on the current state of ProcedureManager.
	Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (ScheduleResult, error)
}
