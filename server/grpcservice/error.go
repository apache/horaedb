// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpcservice

import (
	"github.com/CeresDB/ceresmeta/pkg/coderr"
)

var (
	ErrRecvHeartbeat           = coderr.NewCodeError(coderr.Internal, "receive heartbeat")
	ErrRegisterHeartbeatSender = coderr.NewCodeError(coderr.Internal, "register heartbeat sender")
)
