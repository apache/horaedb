// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

import (
	"context"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	alloc := NewAllocatorImpl(kv, defaultRootPath+defaultAllocIDKey, defaultStep)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	for i := start; i < start+size; i++ {
		value, err := alloc.Alloc(ctx)
		re.NoError(err)
		re.Equal(uint64(i), value)
	}
}
