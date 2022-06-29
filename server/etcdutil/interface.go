// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package etcdutil

type EtcdLeaderGetter interface {
	EtcdLeaderID() uint64
}
