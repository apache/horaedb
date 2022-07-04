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

// fork from: https://github.com/tikv/pd/blob/master/server/storage/kv/etcd_kv.go

package storage

import (
	"path"
	"strings"
	"time"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/pingcap/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

const (
	delimiter = "/"
)

type etcdKV struct {
	client   *clientv3.Client
	rootPath string

	requestTimeout time.Duration
}

// NewEtcdKV creates a new etcd kv.
//nolint
func NewEtcdKV(client *clientv3.Client, rootPath string, requestTimeout time.Duration) *etcdKV {
	return &etcdKV{
		client:         client,
		rootPath:       rootPath,
		requestTimeout: requestTimeout,
	}
}

func (kv *etcdKV) Get(key string) (string, error) {
	key = path.Join(kv.rootPath, key)

	ctx, cancel := context.WithTimeout(context.Background(), kv.requestTimeout)
	defer cancel()

	resp, err := kv.client.Get(ctx, key)
	if err != nil {
		return "", etcdutil.ErrEtcdKVGet.WithCause(err)
	}
	if n := len(resp.Kvs); n == 0 {
		return "", nil
	} else if n > 1 {
		return "", etcdutil.ErrEtcdKVGetResponse.WithCausef("%v", resp.Kvs)
	}
	return string(resp.Kvs[0].Value), nil
}

func (kv *etcdKV) Scan(key, endKey string, limit int) ([]string, []string, error) {
	key = strings.Join([]string{kv.rootPath, key}, delimiter)
	endKey = strings.Join([]string{kv.rootPath, endKey}, delimiter)

	ctx, cancel := context.WithTimeout(context.Background(), kv.requestTimeout)
	defer cancel()

	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(int64(limit))
	resp, err := kv.client.Get(ctx, key, withRange, withLimit)
	if err != nil {
		return nil, nil, etcdutil.ErrEtcdKVGet.WithCause(err)
	}
	keys := make([]string, 0, len(resp.Kvs))
	values := make([]string, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		keys = append(keys, strings.TrimPrefix(strings.TrimPrefix(string(item.Key), kv.rootPath), delimiter))
		values = append(values, string(item.Value))
	}
	return keys, values, nil
}

func (kv *etcdKV) Put(key, value string) error {
	key = strings.Join([]string{kv.rootPath, key}, delimiter)
	ctx, cancel := context.WithTimeout(context.Background(), kv.requestTimeout)
	defer cancel()
	_, err := kv.client.Put(ctx, key, value)
	if err != nil {
		e := etcdutil.ErrEtcdKVPut.WithCause(err)
		log.Error("save to etcd meet error", zap.String("key", key), zap.String("value", value), zap.Error(e))
		return e
	}
	return nil
}

func (kv *etcdKV) Delete(key string) error {
	key = strings.Join([]string{kv.rootPath, key}, delimiter)
	ctx, cancel := context.WithTimeout(context.Background(), kv.requestTimeout)
	defer cancel()
	_, err := kv.client.Delete(ctx, key)
	if err != nil {
		err = etcdutil.ErrEtcdKVDelete.WithCause(err)
		log.Error("remove from etcd meet error", zap.String("key", key), zap.Error(err))
		return err
	}
	return nil
}
