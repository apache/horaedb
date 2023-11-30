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
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/CeresDB/horaedbproto/golang/pkg/metaeventpb"
	"github.com/CeresDB/horaemeta/server/storage"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	shardPath = "shards"
	keySep    = "/"
)

type ShardRegisterEvent struct {
	clusterName   string
	ShardID       storage.ShardID
	NewLeaderNode string
}

type ShardExpireEvent struct {
	clusterName   string
	ShardID       storage.ShardID
	OldLeaderNode string
}

type ShardEventCallback interface {
	OnShardRegistered(ctx context.Context, event ShardRegisterEvent) error
	OnShardExpired(ctx context.Context, event ShardExpireEvent) error
}

type ShardWatch interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	RegisteringEventCallback(eventCallback ShardEventCallback)
}

// EtcdShardWatch used to watch the distributed lock of shard, and provide the corresponding callback function.
type EtcdShardWatch struct {
	logger         *zap.Logger
	clusterName    string
	rootPath       string
	etcdClient     *clientv3.Client
	eventCallbacks []ShardEventCallback

	lock      sync.RWMutex
	isRunning bool
	cancel    context.CancelFunc
}

type NoopShardWatch struct{}

func NewNoopShardWatch() ShardWatch {
	return NoopShardWatch{}
}

func (n NoopShardWatch) Start(_ context.Context) error {
	return nil
}

func (n NoopShardWatch) Stop(_ context.Context) error {
	return nil
}

func (n NoopShardWatch) RegisteringEventCallback(_ ShardEventCallback) {}

func NewEtcdShardWatch(logger *zap.Logger, clusterName string, rootPath string, client *clientv3.Client) ShardWatch {
	return &EtcdShardWatch{
		logger:         logger,
		clusterName:    clusterName,
		rootPath:       rootPath,
		etcdClient:     client,
		eventCallbacks: []ShardEventCallback{},

		lock:      sync.RWMutex{},
		isRunning: false,
		cancel:    nil,
	}
}

func (w *EtcdShardWatch) Start(ctx context.Context) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	shardKeyPrefix := encodeShardKeyPrefix(w.rootPath, w.clusterName, shardPath)
	if err := w.startWatch(ctx, shardKeyPrefix); err != nil {
		return errors.WithMessage(err, "etcd register watch failed")
	}
	if w.isRunning {
		return nil
	}

	w.isRunning = true
	return nil
}

func (w *EtcdShardWatch) Stop(_ context.Context) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.cancel()

	w.isRunning = false
	return nil
}

func (w *EtcdShardWatch) RegisteringEventCallback(eventCallback ShardEventCallback) {
	w.eventCallbacks = append(w.eventCallbacks, eventCallback)
}

func (w *EtcdShardWatch) startWatch(ctx context.Context, path string) error {
	w.logger.Info("register shard watch", zap.String("watchPath", path))
	go func() {
		ctxWithCancel, cancel := context.WithCancel(ctx)
		w.cancel = cancel
		respChan := w.etcdClient.Watch(ctxWithCancel, path, clientv3.WithPrefix(), clientv3.WithPrevKV())
		for resp := range respChan {
			for _, event := range resp.Events {
				if err := w.processEvent(ctx, event); err != nil {
					w.logger.Error("process event", zap.Error(err))
				}
			}
		}
	}()
	return nil
}

func (w *EtcdShardWatch) processEvent(ctx context.Context, event *clientv3.Event) error {
	switch event.Type {
	case mvccpb.DELETE:
		shardID, err := decodeShardKey(string(event.Kv.Key))
		if err != nil {
			return err
		}
		shardLockValue, err := convertShardLockValueToPB(event.PrevKv.Value)
		if err != nil {
			return err
		}
		w.logger.Info("receive delete event", zap.String("preKV", fmt.Sprintf("%v", event.PrevKv)), zap.String("event", fmt.Sprintf("%v", event)), zap.Uint64("shardID", shardID), zap.String("oldLeader", shardLockValue.NodeName))
		for _, callback := range w.eventCallbacks {
			if err := callback.OnShardExpired(ctx, ShardExpireEvent{
				clusterName:   w.clusterName,
				ShardID:       storage.ShardID(shardID),
				OldLeaderNode: shardLockValue.NodeName,
			}); err != nil {
				return err
			}
		}
	case mvccpb.PUT:
		shardID, err := decodeShardKey(string(event.Kv.Key))
		if err != nil {
			return err
		}
		shardLockValue, err := convertShardLockValueToPB(event.Kv.Value)
		if err != nil {
			return err
		}
		w.logger.Info("receive put event", zap.String("event", fmt.Sprintf("%v", event)), zap.Uint64("shardID", shardID), zap.String("oldLeader", shardLockValue.NodeName))
		for _, callback := range w.eventCallbacks {
			if err := callback.OnShardRegistered(ctx, ShardRegisterEvent{
				clusterName:   w.clusterName,
				ShardID:       storage.ShardID(shardID),
				NewLeaderNode: shardLockValue.NodeName,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func decodeShardKey(keyPath string) (uint64, error) {
	pathList := strings.Split(keyPath, keySep)
	shardID, err := strconv.ParseUint(pathList[len(pathList)-1], 10, 64)
	if err != nil {
		return 0, errors.WithMessage(err, "decode etcd event key failed")
	}
	return shardID, nil
}

func encodeShardKeyPrefix(rootPath, shardPath, clusterName string) string {
	return strings.Join([]string{rootPath, shardPath, clusterName}, keySep)
}

func encodeShardKey(rootPath, shardPath, clusterName string, shardID uint64) string {
	shardKeyPrefix := encodeShardKeyPrefix(rootPath, shardPath, clusterName)
	return strings.Join([]string{shardKeyPrefix, strconv.FormatUint(shardID, 10)}, keySep)
}

func convertShardLockValueToPB(value []byte) (*metaeventpb.ShardLockValue, error) {
	shardLockValue := &metaeventpb.ShardLockValue{}
	if err := proto.Unmarshal(value, shardLockValue); err != nil {
		return shardLockValue, errors.WithMessage(err, "unmarshal shardLockValue failed")
	}
	return shardLockValue, nil
}
