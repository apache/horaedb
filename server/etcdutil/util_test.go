// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package etcdutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func makeTestKeys(num int) []string {
	keys := make([]string, 0, num)
	for idx := 0; idx < num; idx++ {
		keys = append(keys, fmt.Sprintf("%010d", idx))
	}

	return keys
}

// Put some keys and scan all of them successfully.
func TestScanNormal(t *testing.T) {
	r := require.New(t)

	_, client, closeSrv := PrepareEtcdServerAndClient(t)
	defer closeSrv()

	keys := makeTestKeys(51)
	lastKey := keys[len(keys)-1]
	keys = keys[0 : len(keys)-1]
	ctx := context.Background()

	// Put the keys.
	for _, key := range keys {
		// Let the value equal key for simplicity.
		val := key
		_, err := client.Put(ctx, key, val)
		r.NoError(err)
	}

	// Scan the keys with different batch size.
	batchSizes := []int{1, 10, 12, 30, 50, 90}
	startKey, endKey := keys[0], lastKey
	collectedKeys := make([]string, 0, len(keys))
	for _, batchSz := range batchSizes {
		collectedKeys = collectedKeys[:0]

		do := func(key string, value []byte) error {
			r.Equal(key, string(value))

			collectedKeys = append(collectedKeys, key)
			return nil
		}
		err := Scan(ctx, client, startKey, endKey, batchSz, do)
		r.NoError(err)

		r.Equal(collectedKeys, keys)
	}
}

// Test the cases where scan fails.
func TestScanFailed(t *testing.T) {
	r := require.New(t)

	_, client, closeSrv := PrepareEtcdServerAndClient(t)
	defer closeSrv()

	keys := makeTestKeys(50)
	ctx := context.Background()

	// Put the keys.
	for _, key := range keys {
		// Let the value equal key for simplicity.
		val := key
		_, err := client.Put(ctx, key, val)
		r.NoError(err)
	}

	fakeErr := fmt.Errorf("fake error for mock failed scan")
	do := func(key string, value []byte) error {
		if key > keys[len(keys)/2] {
			return fakeErr
		}
		return nil
	}
	startKey, endKey := keys[0], keys[len(keys)-1]
	err := Scan(ctx, client, startKey, endKey, 10, do)
	r.Equal(fakeErr, err)
}
