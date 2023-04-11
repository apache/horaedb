// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"

	"github.com/CeresDB/ceresmeta/server/storage"
)

type State string

const (
	StateInit      = "init"
	StateRunning   = "running"
	StateFinished  = "finished"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
)

type Typ uint

const (
	// Cluster Operation
	Create Typ = iota
	Delete
	TransferLeader
	Migrate
	Split
	Merge
	Scatter

	// DDL
	CreateTable
	DropTable
	CreatePartitionTable
	DropPartitionTable
)

type Priority uint32

// Lower value means higher priority.
const (
	PriorityHigh Priority = 3
	PriorityMed  Priority = 5
	PriorityLow  Priority = 10
)

// Procedure is used to describe how to execute a set of operations from the scheduler, e.g. SwitchLeaderProcedure, MergeShardProcedure.
type Procedure interface {
	// ID of the procedure.
	ID() uint64

	// Typ of the procedure.
	Typ() Typ

	// Start the procedure.
	Start(ctx context.Context) error

	// Cancel the procedure.
	Cancel(ctx context.Context) error

	// State of the procedure. Retrieve the state of this procedure.
	State() State

	// RelatedVersionInfo return the related shard and version information corresponding to this procedure for verifying whether the procedure can be executed.
	RelatedVersionInfo() RelatedVersionInfo

	// Priority present the priority of this procedure, the procedure with high level priority will be executed first.
	Priority() Priority
}

// Info is used to provide immutable description procedure information.
type Info struct {
	ID    uint64
	Typ   Typ
	State State
}

type RelatedVersionInfo struct {
	ClusterID storage.ClusterID
	// shardWithVersion return the shardID associated with this procedure.
	ShardWithVersion map[storage.ShardID]uint64
	// clusterVersion return the cluster version when the procedure is created.
	// When performing cluster operation, it is necessary to ensure cluster version consistency.
	ClusterVersion uint64
}
