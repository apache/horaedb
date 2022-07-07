// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.
// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// fork from https://github.com/tikv/pd/blob/master/server/storage/kv/kv_test.go

package storage

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/tempurl"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const defaultRequestTimeout = time.Second * 10

func TestEtcd(t *testing.T) {
	re := require.New(t)
	cfg := newTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	defer etcd.Close()

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))

	kv := NewEtcdKV(client, rootPath)
	testReadWrite(re, kv)
	testRange(re, kv)
}

func testReadWrite(re *require.Assertions, kv KV) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()
	v, err := kv.Get(ctx, "key")
	re.NoError(err)
	re.Equal("", v)
	err = kv.Put(ctx, "key", "value")
	re.NoError(err)
	v, err = kv.Get(ctx, "key")
	re.NoError(err)
	re.Equal("value", v)
	err = kv.Delete(ctx, "key")
	re.NoError(err)
	v, err = kv.Get(ctx, "key")
	re.NoError(err)
	re.Equal("", v)
	err = kv.Delete(ctx, "key")
	re.NoError(err)
}

func testRange(re *require.Assertions, kv KV) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()
	keys := []string{
		"test-a", "test-a/a", "test-a/ab",
		"test", "test/a", "test/ab",
		"testa", "testa/a", "testa/ab",
	}
	for _, k := range keys {
		err := kv.Put(ctx, k, k)
		re.NoError(err)
	}
	sortedKeys := keys
	sort.Strings(sortedKeys)

	testCases := []struct {
		start, end string
		limit      int
		expect     []string
	}{
		{start: "", end: "z", limit: 100, expect: sortedKeys},
		{start: "", end: "z", limit: 3, expect: sortedKeys[:3]},
		{start: "testa", end: "z", limit: 3, expect: []string{"testa", "testa/a", "testa/ab"}},
		{start: "test/", end: clientv3.GetPrefixRangeEnd("test/"), limit: 100, expect: []string{"test/a", "test/ab"}},
		{start: "test-a/", end: clientv3.GetPrefixRangeEnd("test-a/"), limit: 100, expect: []string{"test-a/a", "test-a/ab"}},
		{start: "test", end: clientv3.GetPrefixRangeEnd("test"), limit: 100, expect: sortedKeys},
		{start: "test", end: clientv3.GetPrefixRangeEnd("test/"), limit: 100, expect: []string{"test", "test-a", "test-a/a", "test-a/ab", "test/a", "test/ab"}},
	}

	for _, tc := range testCases {
		ks, vs, err := kv.Scan(ctx, tc.start, tc.end, tc.limit)
		re.NoError(err)
		re.Equal(tc.expect, ks)
		re.Equal(tc.expect, vs)
	}
}

func newTestSingleConfig(t *testing.T) *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = "test_etcd"
	cfg.Dir = t.TempDir()
	cfg.WalDir = ""
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"stdout"}

	pu, _ := url.Parse(tempurl.Alloc())
	cfg.LPUrls = []url.URL{*pu}
	cfg.APUrls = cfg.LPUrls
	cu, _ := url.Parse(tempurl.Alloc())
	cfg.LCUrls = []url.URL{*cu}
	cfg.ACUrls = cfg.LCUrls

	cfg.StrictReconfigCheck = false
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.LPUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}
