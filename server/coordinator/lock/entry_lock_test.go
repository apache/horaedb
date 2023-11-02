/*
 * Copyright 2022 The CeresDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
