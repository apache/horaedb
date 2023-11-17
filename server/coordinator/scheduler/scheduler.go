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

package scheduler

import (
	"context"

	"github.com/CeresDB/horaemeta/server/cluster/metadata"
	"github.com/CeresDB/horaemeta/server/coordinator/procedure"
	"github.com/CeresDB/horaemeta/server/storage"
)

type ScheduleResult struct {
	Procedure procedure.Procedure
	// The reason that the procedure is generated for.
	Reason string
}

type ShardAffinity struct {
	ShardID               storage.ShardID `json:"shardID"`
	NumAllowedOtherShards uint            `json:"numAllowedOtherShards"`
}

type ShardAffinityRule struct {
	Affinities []ShardAffinity
}

type Scheduler interface {
	Name() string
	// Schedule will generate procedure based on current cluster snapshot, which will be submitted to ProcedureManager, and whether it is actually executed depends on the current state of ProcedureManager.
	Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (ScheduleResult, error)
	// UpdateEnableSchedule is used to update enableSchedule for scheduler,
	// EnableSchedule means that the cluster topology is locked and the mapping between shards and nodes cannot be changed.
	UpdateEnableSchedule(ctx context.Context, enable bool)
	AddShardAffinityRule(ctx context.Context, rule ShardAffinityRule) error
	RemoveShardAffinityRule(ctx context.Context, shardID storage.ShardID) error
	ListShardAffinityRule(ctx context.Context) (ShardAffinityRule, error)
}
