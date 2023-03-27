// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package lock

import (
	"fmt"
	"sync"
)

type EntryLock struct {
	lock       sync.Mutex
	entryLocks map[uint64]struct{}
}

func NewEntryLock(initCapacity int) EntryLock {
	return EntryLock{
		entryLocks: make(map[uint64]struct{}, initCapacity),
	}
}

func (l *EntryLock) TryLock(locks []uint64) bool {
	l.lock.Lock()
	defer l.lock.Unlock()

	for _, lock := range locks {
		_, exists := l.entryLocks[lock]
		if exists {
			return false
		}
	}

	for _, lock := range locks {
		l.entryLocks[lock] = struct{}{}
	}

	return true
}

func (l *EntryLock) UnLock(locks []uint64) {
	l.lock.Lock()
	defer l.lock.Unlock()

	for _, lock := range locks {
		_, exists := l.entryLocks[lock]
		if !exists {
			panic(fmt.Sprintf("try to unlock nonexistent lock, exists locks:%v, unlock locks:%v", l.entryLocks, locks))
		}
	}

	for _, lock := range locks {
		delete(l.entryLocks, lock)
	}

}
