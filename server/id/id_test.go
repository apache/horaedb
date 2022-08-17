// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

import (
	"context"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const (
	defaultRequestTimeout = time.Second * 30
	defaultStep           = 100
	defaultRootPath       = "/ceresmeta"
	defaultAllocIDKey     = "/id"
)

func newTestKV(t *testing.T) clientv3.KV {
	re := require.New(t)
	cfg := etcdutil.NewTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	<-etcd.Server.ReadyNotify()

	endpoint := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint},
	})
	re.NoError(err)

	return client
}

func TestMultipleAllocBasedOnKV(t *testing.T) {
	start := 0
	size := 201
	kv := newTestKV(t)

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
