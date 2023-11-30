/*
 * Copyright 2022 The HoraeDB Authors
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

import (
	"go.etcd.io/etcd/server/v3/etcdserver"
)

type EtcdLeaderGetter interface {
	EtcdLeaderID() (uint64, error)
}

type LeaderGetterWrapper struct {
	Server *etcdserver.EtcdServer
}

func (w *LeaderGetterWrapper) EtcdLeaderID() (uint64, error) {
	return w.Server.Lead(), nil
}
