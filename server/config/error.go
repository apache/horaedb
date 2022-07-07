// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package config

import (
	"github.com/CeresDB/ceresmeta/pkg/coderr"
)

var (
	ErrHelpRequested      = coderr.NewCodeError(coderr.PrintHelpUsage, "help requested")
	ErrInvalidPeerURL     = coderr.NewCodeError(coderr.InvalidParams, "invalid peers url")
	ErrInvalidCommandArgs = coderr.NewCodeError(coderr.InvalidParams, "invalid command arguments")
	ErrRetrieveHostname   = coderr.NewCodeError(coderr.Internal, "retrieve local hostname")
)
