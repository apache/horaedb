// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package manager

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var ErrInvalidTopologyType = coderr.NewCodeError(coderr.InvalidParams, "invalid topology type")
