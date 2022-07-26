// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

import "context"

// Allocator defines the id allocator on the ceresdb cluster meta info.
type Allocator interface {
	// Alloc allocs a unique id.
	Alloc(ctx context.Context) (uint64, error)
}
