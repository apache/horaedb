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

package watch

import (
	"context"
	"testing"
	"time"

	"github.com/CeresDB/horaedbproto/golang/pkg/metaeventpb"
	"github.com/CeresDB/horaemeta/server/etcdutil"
	"github.com/CeresDB/horaemeta/server/storage"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	TestClusterName = "defaultCluster"
	TestRootPath    = "/rootPath"
	TestShardPath   = "shards"
	TestShardID     = 1
	TestNodeName    = "testNode"
)

func TestWatch(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	_, client, _ := etcdutil.PrepareEtcdServerAndClient(t)
	watch := NewEtcdShardWatch(zap.NewNop(), TestClusterName, TestRootPath, client)
	err := watch.Start(ctx)
	re.NoError(err)

	testCallback := testShardEventCallback{
		result: 0,
		re:     re,
	}

	watch.RegisteringEventCallback(&testCallback)

	// Valid that callback function is executed and the params are as expected.
	b, err := proto.Marshal(&metaeventpb.ShardLockValue{NodeName: TestNodeName})
	re.NoError(err)

	keyPath := encodeShardKey(TestRootPath, TestClusterName, TestShardPath, TestShardID)
	_, err = client.Put(ctx, keyPath, string(b))
	re.NoError(err)
	time.Sleep(time.Millisecond * 10)
	re.Equal(2, testCallback.result)

	_, err = client.Delete(ctx, keyPath, clientv3.WithPrevKV())
	re.NoError(err)
	time.Sleep(time.Millisecond * 10)
	re.Equal(1, testCallback.result)
}

type testShardEventCallback struct {
	result int
	re     *require.Assertions
}

func (c *testShardEventCallback) OnShardRegistered(_ context.Context, event ShardRegisterEvent) error {
	c.result = 2
	c.re.Equal(storage.ShardID(TestShardID), event.ShardID)
	c.re.Equal(TestNodeName, event.NewLeaderNode)
	return nil
}

func (c *testShardEventCallback) OnShardExpired(_ context.Context, event ShardExpireEvent) error {
	c.result = 1
	c.re.Equal(storage.ShardID(TestShardID), event.ShardID)
	c.re.Equal(TestNodeName, event.OldLeaderNode)
	return nil
}
