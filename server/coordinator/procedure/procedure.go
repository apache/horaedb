/*
 * Copyright 2022 The CeresDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package procedure

import (
	"context"

	"github.com/CeresDB/horaemeta/server/storage"
)

type State string

const (
	StateInit      = "init"
	StateRunning   = "running"
	StateFinished  = "finished"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
)

type Kind uint

const (
	// Cluster Operation
	Create Kind = iota
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

	// Kind of the procedure.
	Kind() Kind

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
	Kind  Kind
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
