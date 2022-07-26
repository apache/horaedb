// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

import (
	"context"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const defaultRequestTimeout = time.Second * 10

// TODO: need a concurrent test case
func TestAlloc(t *testing.T) {
	re := require.New(t)
	cfg := etcdutil.NewTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)

	<-etcd.Server.ReadyNotify()

	endpoint := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint},
	})
	assert.NoError(t, err)
	rootPath := path.Join("/ceresmeta", strconv.FormatUint(100, 10))
	kv := storage.NewEtcdKV(client, rootPath)

	alloc := NewAllocatorImpl(kv, rootPath, "id")
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()
	for i := 0; i < 2010; i++ {
		value, err := alloc.Alloc(ctx)
		re.NoError(err)
		re.Equal(uint64(i+1), value)
	}
}
