// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package schedule

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrStreamNotAvailable     = coderr.NewCodeError(coderr.Internal, "stream to node is not available")
	ErrStreamSendMsg          = coderr.NewCodeError(coderr.Internal, "send msg by stream to node")
	ErrStreamSendTimeout      = coderr.NewCodeError(coderr.Internal, "send msg timeout")
	ErrHeartbeatStreamsClosed = coderr.NewCodeError(coderr.Internal, "HeartbeatStreams closed")
)
