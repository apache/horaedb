// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
)

type Write interface {
	CreateOrUpdate(ctx context.Context, meta *Meta) error
}

type Meta struct {
	ID      uint64
	Typ     Typ
	State   State
	RawData []byte
}

// TODO: move needRetry to scheduler.go.
func (m *Meta) needRetry() bool {
	if m.State == StateCancelled || m.State == StateFinished {
		return false
	}
	return true
}

type Storage interface {
	Write
	List(ctx context.Context, batchSize int) ([]*Meta, error)
	MarkDeleted(ctx context.Context, id uint64) error
}
