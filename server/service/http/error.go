// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package http

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrParseRequest  = coderr.NewCodeError(coderr.BadRequest, "parse request params failed")
	ErrParseResponse = coderr.NewCodeError(coderr.Internal, "error marshaling json response")
)
