// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package etcdutil

import (
	"context"

	"github.com/CeresDB/ceresmeta/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func Get(ctx context.Context, client *clientv3.Client, key string) (string, error) {
	resp, err := client.Get(ctx, key)
	if err != nil {
		return "", ErrEtcdKVGet.WithCause(err)
	}
	if n := len(resp.Kvs); n == 0 {
		return "", ErrEtcdKVGetNotFound
	} else if n > 1 {
		return "", ErrEtcdKVGetResponse.WithCausef("%v", resp.Kvs)
	}

	return string(resp.Kvs[0].Value), nil
}

func Scan(ctx context.Context, client *clientv3.Client, startKey, endKey string, batchSize int, do func(key string, val []byte) error) error {
	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(int64(batchSize))

	// Take a special process for the first batch.
	resp, err := client.Get(ctx, startKey, withRange, withLimit)
	if err != nil {
		return ErrEtcdKVGet.WithCause(err)
	}
	if len(resp.Kvs) == 0 {
		return nil
	}

	doIfNotEndKey := func(key, val []byte) error {
		// TODO: avoid such a copy on key.
		keyStr := string(key)
		if keyStr == endKey {
			return nil
		}

		return do(keyStr, val)
	}

	for _, item := range resp.Kvs {
		err := doIfNotEndKey(item.Key, item.Value)
		if err != nil {
			return err
		}
	}

	lastKeyInPrevBatch := string(resp.Kvs[len(resp.Kvs)-1].Key)
	// The following batches always contain one key in the previous batch, so we have to increment the batchSize to batchSize + 1;
	withLimit = clientv3.WithLimit(int64(batchSize + 1))
	for {
		if lastKeyInPrevBatch == endKey {
			log.Warn("Stop scanning because the end key is reached", zap.String("endKey", endKey))
			return nil
		}
		startKey = lastKeyInPrevBatch

		// Get the keys range [startKey, endKey).
		resp, err := client.Get(ctx, startKey, withRange, withLimit)
		if err != nil {
			return ErrEtcdKVGet.WithCause(err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if len(resp.Kvs) <= 1 {
			// The only one key is `startKey` which is actually processed already.
			return nil
		}

		// Skip the first key which is processed already.
		for _, item := range resp.Kvs[1:] {
			err := doIfNotEndKey(item.Key, item.Value)
			if err != nil {
				return err
			}
		}

		// Check whether the keys is exhausted.
		if len(resp.Kvs) < batchSize {
			return nil
		}

		lastKeyInPrevBatch = string(resp.Kvs[len(resp.Kvs)-1].Key)
	}
}
