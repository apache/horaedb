/*
 * Copyright 2022 The HoraeDB Authors
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

package member

import (
	"context"
	"testing"
	"time"

	"github.com/CeresDB/horaemeta/server/etcdutil"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
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

func (ctx *mockWatchCtx) EtcdLeaderID() (uint64, error) {
	return ctx.srv.Lead(), nil
}

func TestWatchLeaderSingle(t *testing.T) {
	etcd, client, closeSrv := etcdutil.PrepareEtcdServerAndClient(t)
	defer closeSrv()

	watchCtx := &mockWatchCtx{
		stopped: false,
		client:  client,
		srv:     etcd.Server,
	}
	leaderGetter := &etcdutil.LeaderGetterWrapper{Server: etcd.Server}
	rpcTimeout := time.Duration(10) * time.Second
	leaseTTLSec := int64(1)
	mem := NewMember("", uint64(etcd.Server.ID()), "mem0", "", client, leaderGetter, rpcTimeout)
	leaderWatcher := NewLeaderWatcher(watchCtx, mem, leaseTTLSec, true)

	ctx, cancelWatch := context.WithCancel(context.Background())
	watchedDone := make(chan struct{}, 1)
	go func() {
		leaderWatcher.Watch(ctx, nil)
		watchedDone <- struct{}{}
	}()

	// Wait for watcher starting
	// TODO: This unit test may fail. Currently, it is solved by increasing the sleep time, and the code needs to be optimized in the future.
	time.Sleep(time.Duration(2000) * time.Millisecond)

	// check the member has been the leader
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := mem.getLeader(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, resp.Leader.Id, mem.ID)

	// cancel the watch
	cancelWatch()
	<-watchedDone

	// check again whether the leader should be reset
	ctx, cancel = context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err = mem.getLeader(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Leader)
}
