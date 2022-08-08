// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpcservice

import (
	"github.com/CeresDB/ceresmeta/pkg/coderr"
)

var (
	ErrRecvHeartbeat         = coderr.NewCodeError(coderr.Internal, "receive heartbeat")
	ErrBindHeartbeatStream   = coderr.NewCodeError(coderr.Internal, "bind heartbeat sender")
	ErrUnbindHeartbeatStream = coderr.NewCodeError(coderr.Internal, "unbind heartbeat sender")
	ErrParseURL              = coderr.NewCodeError(coderr.Internal, "parse url")
	ErrGRPCDial              = coderr.NewCodeError(coderr.Internal, "grpc dial")
	ErrForward               = coderr.NewCodeError(coderr.Internal, "grpc forward")
)
