// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAlloc(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	allocor := NewReusableAllocatorImpl([]uint64{}, uint64(1))
	// IDs: []
	// Alloc: 1
	// IDs: [1]
	id, err := allocor.Alloc(ctx)
	re.NoError(err)
	re.Equal(uint64(1), id)

	// IDs: [1]
	// Alloc: 2
	// IDs: [1,2]
	id, err = allocor.Alloc(ctx)
	re.NoError(err)
	re.Equal(uint64(2), id)

	// IDs: [1,2]
	// Collect: 2
	// IDs: [1]
	err = allocor.Collect(ctx, uint64(2))
	re.NoError(err)

	// IDs: [1]
	// Alloc: 2
	// IDs: [1,2]
	id, err = allocor.Alloc(ctx)
	re.NoError(err)
	re.Equal(uint64(2), id)

	// IDs: [1,2,3,5,6]
	allocor = NewReusableAllocatorImpl([]uint64{1, 2, 3, 5, 6}, uint64(1))

	// IDs: [1,2,3,5,6]
	// Alloc: 4
	// IDs: [1,2,3,4,5,6]
	id, err = allocor.Alloc(ctx)
	re.NoError(err)
	re.Equal(uint64(4), id)

	// IDs: [1,2,3,4,5,6]
	// Alloc: 7
	// IDs: [1,2,3,4,5,6,7]
	id, err = allocor.Alloc(ctx)
	re.NoError(err)
	re.Equal(uint64(7), id)

	// IDs: [1,2,3,4,5,6,7]
	// Collect: 1
	// IDs: [2,3,4,5,6,7]
	err = allocor.Collect(ctx, uint64(1))
	re.NoError(err)

	// IDs: [2,3,4,5,6,7]
	// Alloc: 1
	// IDs: [1,2,3,4,5,6,7]
	id, err = allocor.Alloc(ctx)
	re.NoError(err)
	re.Equal(uint64(1), id)
}
