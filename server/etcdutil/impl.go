// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package etcdutil

import (
	"go.etcd.io/etcd/server/v3/etcdserver"
)

type LeaderGetterWrapper struct {
	Server *etcdserver.EtcdServer
}

func (w *LeaderGetterWrapper) EtcdLeaderID() uint64 {
	return w.Server.Lead()
}
