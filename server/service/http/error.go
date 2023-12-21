/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package http

import "github.com/apache/incubator-horaedb-meta/pkg/coderr"

var (
	ErrParseRequest                  = coderr.NewCodeError(coderr.BadRequest, "parse request params")
	ErrInvalidParamsForCreateCluster = coderr.NewCodeError(coderr.BadRequest, "invalid params to create cluster")
	ErrTable                         = coderr.NewCodeError(coderr.Internal, "table")
	ErrRoute                         = coderr.NewCodeError(coderr.Internal, "route table")
	ErrGetNodeShards                 = coderr.NewCodeError(coderr.Internal, "get node shards")
	ErrCreateProcedure               = coderr.NewCodeError(coderr.Internal, "create procedure")
	ErrSubmitProcedure               = coderr.NewCodeError(coderr.Internal, "submit procedure")
	ErrGetCluster                    = coderr.NewCodeError(coderr.Internal, "get cluster")
	ErrAllocShardID                  = coderr.NewCodeError(coderr.Internal, "alloc shard id")
	ErrForwardToLeader               = coderr.NewCodeError(coderr.Internal, "forward to leader")
	ErrParseLeaderAddr               = coderr.NewCodeError(coderr.Internal, "parse leader addr")
	ErrHealthCheck                   = coderr.NewCodeError(coderr.Internal, "server health check")
	ErrParseTopology                 = coderr.NewCodeError(coderr.Internal, "parse topology type")
	ErrUpdateFlowLimiter             = coderr.NewCodeError(coderr.Internal, "update flow limiter")
	ErrGetEnableSchedule             = coderr.NewCodeError(coderr.Internal, "get enableSchedule")
	ErrUpdateEnableSchedule          = coderr.NewCodeError(coderr.Internal, "update enableSchedule")
	ErrAddLearner                    = coderr.NewCodeError(coderr.Internal, "add member as learner")
	ErrListMembers                   = coderr.NewCodeError(coderr.Internal, "get member list")
	ErrRemoveMembers                 = coderr.NewCodeError(coderr.Internal, "remove member")
	ErrGetMember                     = coderr.NewCodeError(coderr.Internal, "get member")
	ErrListAffinityRules             = coderr.NewCodeError(coderr.Internal, "list affinity rules")
	ErrAddAffinityRule               = coderr.NewCodeError(coderr.Internal, "add affinity rule")
	ErrRemoveAffinityRule            = coderr.NewCodeError(coderr.Internal, "remove affinity rule")
)
