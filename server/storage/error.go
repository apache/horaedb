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

package storage

import "github.com/CeresDB/horaemeta/pkg/coderr"

var (
	ErrEncode = coderr.NewCodeError(coderr.Internal, "storage encode")
	ErrDecode = coderr.NewCodeError(coderr.Internal, "storage decode")

	ErrCreateSchemaAgain         = coderr.NewCodeError(coderr.Internal, "storage create schemas")
	ErrCreateClusterAgain        = coderr.NewCodeError(coderr.Internal, "storage create cluster")
	ErrUpdateCluster             = coderr.NewCodeError(coderr.Internal, "storage update cluster")
	ErrCreateClusterViewAgain    = coderr.NewCodeError(coderr.Internal, "storage create cluster view")
	ErrUpdateClusterViewConflict = coderr.NewCodeError(coderr.Internal, "storage update cluster view")
	ErrCreateTableAgain          = coderr.NewCodeError(coderr.Internal, "storage create tables")
	ErrDeleteTableAgain          = coderr.NewCodeError(coderr.Internal, "storage delete table")
	ErrCreateShardViewAgain      = coderr.NewCodeError(coderr.Internal, "storage create shard view")
	ErrUpdateShardViewConflict   = coderr.NewCodeError(coderr.Internal, "storage update shard view")
)
