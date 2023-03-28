// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package queue

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrQueueFull               = coderr.NewCodeError(coderr.Internal, "queue is full, unable to offer more data")
	ErrPushDuplicatedProcedure = coderr.NewCodeError(coderr.Internal, "try to push duplicated procedure")
)
