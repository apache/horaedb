// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrNodeNumberNotEnough = coderr.NewCodeError(coderr.Internal, "node number not enough")
	ErrPickNode            = coderr.NewCodeError(coderr.Internal, "no node is picked")
)
