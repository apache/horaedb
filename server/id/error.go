// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrTxnPutEndID = coderr.NewCodeError(coderr.Internal, "put end id in txn")
	ErrAllocID     = coderr.NewCodeError(coderr.Internal, "alloc id")
)
