// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var ErrLockShard = coderr.NewCodeError(coderr.Internal, "lock shard failed")
