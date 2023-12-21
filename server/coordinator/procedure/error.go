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

package procedure

import "github.com/apache/incubator-horaedb-meta/pkg/coderr"

var (
	ErrShardLeaderNotFound     = coderr.NewCodeError(coderr.Internal, "shard leader not found")
	ErrProcedureNotFound       = coderr.NewCodeError(coderr.Internal, "procedure not found")
	ErrClusterConfigChanged    = coderr.NewCodeError(coderr.Internal, "cluster config changed")
	ErrTableNotExists          = coderr.NewCodeError(coderr.Internal, "table not exists")
	ErrTableAlreadyExists      = coderr.NewCodeError(coderr.Internal, "table already exists")
	ErrListRunningProcedure    = coderr.NewCodeError(coderr.Internal, "procedure type not match")
	ErrListProcedure           = coderr.NewCodeError(coderr.Internal, "list running procedure")
	ErrDecodeRawData           = coderr.NewCodeError(coderr.Internal, "decode raw data")
	ErrEncodeRawData           = coderr.NewCodeError(coderr.Internal, "encode raw data")
	ErrGetRequest              = coderr.NewCodeError(coderr.Internal, "get request from event")
	ErrNodeNumberNotEnough     = coderr.NewCodeError(coderr.Internal, "node number not enough")
	ErrEmptyPartitionNames     = coderr.NewCodeError(coderr.Internal, "partition names is empty")
	ErrDropTableResult         = coderr.NewCodeError(coderr.Internal, "length of shard not correct")
	ErrPickShard               = coderr.NewCodeError(coderr.Internal, "pick shard failed")
	ErrSubmitProcedure         = coderr.NewCodeError(coderr.Internal, "submit new procedure")
	ErrQueueFull               = coderr.NewCodeError(coderr.Internal, "queue is full, unable to offer more data")
	ErrPushDuplicatedProcedure = coderr.NewCodeError(coderr.Internal, "try to push duplicated procedure")
	ErrShardNumberNotEnough    = coderr.NewCodeError(coderr.Internal, "shard number not enough")
	ErrEmptyBatchProcedure     = coderr.NewCodeError(coderr.Internal, "procedure batch is empty")
	ErrMergeBatchProcedure     = coderr.NewCodeError(coderr.Internal, "failed to merge procedures batch")
)
