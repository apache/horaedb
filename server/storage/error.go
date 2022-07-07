// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var ErrMetaGetSchemas = coderr.NewCodeError(coderr.Internal, "meta storage get schemas")
