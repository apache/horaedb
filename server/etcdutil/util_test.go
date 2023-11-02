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
