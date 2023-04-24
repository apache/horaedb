// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package watch

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaeventpb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/storage"
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

// ShardWatch used to watch the distributed lock of shard, and provide the corresponding callback function.
type ShardWatch struct {
	clusterName    string
	rootPath       string
	etcdClient     *clientv3.Client
	eventCallbacks []ShardEventCallback

	lock      sync.RWMutex
	isRunning bool
	cancel    context.CancelFunc
}

func NewWatch(clusterName string, rootPath string, client *clientv3.Client) *ShardWatch {
	return &ShardWatch{
		clusterName:    clusterName,
		rootPath:       rootPath,
		etcdClient:     client,
		eventCallbacks: []ShardEventCallback{},
	}
}

func (w *ShardWatch) Start(ctx context.Context) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.isRunning {
		return nil
	}

	shardKeyPrefix := encodeShardKeyPrefix(w.rootPath, w.clusterName, shardPath)
	if err := w.startWatch(ctx, shardKeyPrefix); err != nil {
		return errors.WithMessage(err, "etcd register watch failed")
	}

	w.isRunning = true
	return nil
}

func (w *ShardWatch) Stop(_ context.Context) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.isRunning = false
	w.cancel()
	return nil
}

func (w *ShardWatch) RegisteringEventCallback(eventCallback ShardEventCallback) {
	w.eventCallbacks = append(w.eventCallbacks, eventCallback)
}

func (w *ShardWatch) startWatch(ctx context.Context, path string) error {
	log.Info("register shard watch", zap.String("watchPath", path))
	go func() {
		ctxWithCancel, cancel := context.WithCancel(ctx)
		w.cancel = cancel
		respChan := w.etcdClient.Watch(ctxWithCancel, path, clientv3.WithPrefix(), clientv3.WithPrevKV())
		for resp := range respChan {
			for _, event := range resp.Events {
				if err := w.processEvent(ctx, event); err != nil {
					log.Error("process event", zap.Error(err))
				}
			}
		}
	}()
	return nil
}

func (w *ShardWatch) processEvent(ctx context.Context, event *clientv3.Event) error {
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
		log.Info("receive delete event", zap.String("preKV", fmt.Sprintf("%v", event.PrevKv)), zap.String("event", fmt.Sprintf("%v", event)), zap.Uint64("shardID", shardID), zap.String("oldLeader", shardLockValue.NodeName))
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
		log.Info("receive put event", zap.String("event", fmt.Sprintf("%v", event)), zap.Uint64("shardID", shardID), zap.String("oldLeader", shardLockValue.NodeName))
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
