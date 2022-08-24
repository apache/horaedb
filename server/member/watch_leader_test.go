// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package member

import (
	"context"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver"
)

type mockWatchCtx struct {
	stopped bool
	client  *clientv3.Client
	srv     *etcdserver.EtcdServer
}

func (ctx *mockWatchCtx) ShouldStop() bool {
	return ctx.stopped
}

func (ctx *mockWatchCtx) EtcdLeaderID() uint64 {
	return ctx.srv.Lead()
}

func prepareEtcdServerAndClient(t *testing.T) (*embed.Etcd, *clientv3.Client, func()) {
	cfg := etcdutil.NewTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)

	<-etcd.Server.ReadyNotify()

	endpoint := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint},
	})
	assert.NoError(t, err)

	clean := func() {
		etcd.Close()
		etcdutil.CleanConfig(cfg)
	}
	return etcd, client, clean
}

func TestWatchLeaderSingle(t *testing.T) {
	etcd, client, clean := prepareEtcdServerAndClient(t)
	defer clean()

	watchCtx := &mockWatchCtx{
		stopped: false,
		client:  client,
		srv:     etcd.Server,
	}
	leaderGetter := &etcdutil.LeaderGetterWrapper{Server: etcd.Server}
	rpcTimeout := time.Duration(10) * time.Second
	leaseTTLSec := int64(1)
	mem := NewMember("", uint64(etcd.Server.ID()), "mem0", client, leaderGetter, rpcTimeout)
	leaderWatcher := NewLeaderWatcher(watchCtx, mem, leaseTTLSec)

	ctx, cancelWatch := context.WithCancel(context.Background())
	watchedDone := make(chan struct{}, 1)
	go func() {
		leaderWatcher.Watch(ctx, nil)
		watchedDone <- struct{}{}
	}()

	// Wait for watcher starting
	time.Sleep(time.Duration(200) * time.Millisecond)

	// check the member has been the leader
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := mem.GetLeader(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, resp.Leader.Id, mem.ID)

	// cancel the watch
	cancelWatch()
	<-watchedDone

	// check again whether the leader should be reset
	ctx, cancel = context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err = mem.GetLeader(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Leader)
}
