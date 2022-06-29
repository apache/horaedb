// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package member

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrResetLeader        = coderr.NewCodeError(coderr.Internal, "reset leader by deleting leader key")
	ErrGetLeader          = coderr.NewCodeError(coderr.Internal, "get leader by querying leader key")
	ErrTxnPutLeader       = coderr.NewCodeError(coderr.Internal, "put leader key in txn")
	ErrMultipleLeader     = coderr.NewCodeError(coderr.Internal, "multiple leaders found")
	ErrInvalidLeaderValue = coderr.NewCodeError(coderr.Internal, "invalid leader value")
	ErrMarshalMember      = coderr.NewCodeError(coderr.Internal, "marshal member information")
	ErrGrantLease         = coderr.NewCodeError(coderr.Internal, "grant lease")
	ErrRevokeLease        = coderr.NewCodeError(coderr.Internal, "revoke lease")
	ErrCloseLease         = coderr.NewCodeError(coderr.Internal, "close lease")
)
