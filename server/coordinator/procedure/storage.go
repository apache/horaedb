// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
)

type Write interface {
	CreateOrUpdate(ctx context.Context, meta *Meta) error
}

// nolint
type Meta struct {
	ID      uint64
	Typ     Typ
	State   State
	RawData []byte
}

type Storage interface {
	Write
	Scan(ctx context.Context, batchSize uint, state State, meta []*Meta) error
}
