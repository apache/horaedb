// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package nodepicker

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var ErrNoAliveNodes = coderr.NewCodeError(coderr.InvalidParams, "no alive nodes is found")
