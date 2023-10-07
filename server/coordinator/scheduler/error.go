// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var ErrInvalidTopologyType = coderr.NewCodeError(coderr.Internal, "invalid topology type")
