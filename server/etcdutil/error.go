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

package etcdutil

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrEtcdKVGet         = coderr.NewCodeError(coderr.Internal, "etcd KV get failed")
	ErrEtcdKVGetResponse = coderr.NewCodeError(coderr.Internal, "etcd invalid get value response must only one")
	ErrEtcdKVGetNotFound = coderr.NewCodeError(coderr.Internal, "etcd KV get value not found")
)
