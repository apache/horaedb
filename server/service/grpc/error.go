// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpc

import (
	"github.com/CeresDB/ceresmeta/pkg/coderr"
)

var (
	ErrRecvHeartbeat         = coderr.NewCodeError(coderr.Internal, "receive heartbeat")
	ErrBindHeartbeatStream   = coderr.NewCodeError(coderr.Internal, "bind heartbeat sender")
	ErrUnbindHeartbeatStream = coderr.NewCodeError(coderr.Internal, "unbind heartbeat sender")
	ErrForward               = coderr.NewCodeError(coderr.Internal, "grpc forward")
	ErrFlowLimit             = coderr.NewCodeError(coderr.TooManyRequests, "flow limit")
)
