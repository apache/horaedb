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

package metadata

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrCreateCluster        = coderr.NewCodeError(coderr.BadRequest, "create cluster")
	ErrUpdateCluster        = coderr.NewCodeError(coderr.Internal, "update cluster")
	ErrStartCluster         = coderr.NewCodeError(coderr.Internal, "start cluster")
	ErrClusterAlreadyExists = coderr.NewCodeError(coderr.ClusterAlreadyExists, "cluster already exists")
	ErrClusterNotFound      = coderr.NewCodeError(coderr.NotFound, "cluster not found")
	ErrClusterStateInvalid  = coderr.NewCodeError(coderr.Internal, "cluster state invalid")
	ErrSchemaNotFound       = coderr.NewCodeError(coderr.NotFound, "schema not found")
	ErrTableNotFound        = coderr.NewCodeError(coderr.NotFound, "table not found")
	ErrShardNotFound        = coderr.NewCodeError(coderr.NotFound, "shard not found")
	ErrVersionNotFound      = coderr.NewCodeError(coderr.NotFound, "version not found")
	ErrNodeNotFound         = coderr.NewCodeError(coderr.NotFound, "NodeName not found")
	ErrTableAlreadyExists   = coderr.NewCodeError(coderr.Internal, "table already exists")
	ErrOpenTable            = coderr.NewCodeError(coderr.Internal, "open table")
	ErrParseTopologyType    = coderr.NewCodeError(coderr.Internal, "parse topology type")
)
