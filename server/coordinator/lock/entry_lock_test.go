// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package lock

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntryLock(t *testing.T) {
	re := require.New(t)

	lock := NewEntryLock(3)

	lock1 := []uint64{1}
	result := lock.TryLock(lock1)
	re.Equal(true, result)
	result = lock.TryLock(lock1)
	re.Equal(false, result)
	lock.UnLock(lock1)
	result = lock.TryLock(lock1)
	re.Equal(true, result)
	lock.UnLock(lock1)

	lock2 := []uint64{2, 3, 4}
	lock3 := []uint64{3, 4, 5}
	result = lock.TryLock(lock2)
	re.Equal(true, result)
	result = lock.TryLock(lock2)
	re.Equal(false, result)
	result = lock.TryLock(lock3)
	re.Equal(false, result)
	lock.UnLock(lock2)
	result = lock.TryLock(lock2)
	re.Equal(true, result)
	lock.UnLock(lock2)

	re.Panics(func() {
		lock.UnLock(lock2)
	}, "this function did not panic")
}
