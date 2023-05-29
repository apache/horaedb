// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package server

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrCreateEtcdClient    = coderr.NewCodeError(coderr.Internal, "create etcd etcdCli")
	ErrStartEtcd           = coderr.NewCodeError(coderr.Internal, "start embed etcd")
	ErrStartEtcdTimeout    = coderr.NewCodeError(coderr.Internal, "start etcd server timeout")
	ErrStartServer         = coderr.NewCodeError(coderr.Internal, "start server")
	ErrFlowLimiterNotFound = coderr.NewCodeError(coderr.Internal, "flow limiter not found")
)
