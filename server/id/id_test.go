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

package id

import (
	"context"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	defaultRequestTimeout = time.Second * 30
	defaultStep           = 100
	defaultRootPath       = "/ceresmeta"
	defaultAllocIDKey     = "/id"
)

func TestMultipleAllocBasedOnKV(t *testing.T) {
	start := 0
	size := 201
	_, kv, closeSrv := etcdutil.PrepareEtcdServerAndClient(t)
	defer closeSrv()

	testAllocIDValue(t, kv, start, size)
	testAllocIDValue(t, kv, ((start+size)/defaultStep+1)*defaultStep, size)
}

func testAllocIDValue(t *testing.T, kv clientv3.KV, start, size int) {
	re := require.New(t)
	alloc := NewAllocatorImpl(zap.NewNop(), kv, defaultRootPath+defaultAllocIDKey, defaultStep)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	for i := start; i < start+size; i++ {
		value, err := alloc.Alloc(ctx)
		re.NoError(err)
		re.Equal(uint64(i), value)
	}
}
