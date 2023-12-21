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

package eventdispatch

import (
	"context"

	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
)

type Dispatch interface {
	OpenShard(context context.Context, address string, request OpenShardRequest) error
	CloseShard(context context.Context, address string, request CloseShardRequest) error
	CreateTableOnShard(context context.Context, address string, request CreateTableOnShardRequest) (uint64, error)
	DropTableOnShard(context context.Context, address string, request DropTableOnShardRequest) (uint64, error)
	OpenTableOnShard(ctx context.Context, address string, request OpenTableOnShardRequest) error
	CloseTableOnShard(context context.Context, address string, request CloseTableOnShardRequest) error
}

type OpenShardRequest struct {
	Shard metadata.ShardInfo
}

type CloseShardRequest struct {
	ShardID uint32
}

type UpdateShardInfo struct {
	CurrShardInfo metadata.ShardInfo
}

type CreateTableOnShardRequest struct {
	UpdateShardInfo  UpdateShardInfo
	TableInfo        metadata.TableInfo
	EncodedSchema    []byte
	Engine           string
	CreateIfNotExist bool
	Options          map[string]string
}

type DropTableOnShardRequest struct {
	UpdateShardInfo UpdateShardInfo
	TableInfo       metadata.TableInfo
}

type OpenTableOnShardRequest struct {
	UpdateShardInfo UpdateShardInfo
	TableInfo       metadata.TableInfo
}

type CloseTableOnShardRequest struct {
	UpdateShardInfo UpdateShardInfo
	TableInfo       metadata.TableInfo
}
