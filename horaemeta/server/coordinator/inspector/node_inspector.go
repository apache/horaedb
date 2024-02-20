/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package inspector

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"go.uber.org/zap"
)

const inspectInterval = time.Second * 5

// NodeInspector will inspect node status and remove expired data.
type NodeInspector struct {
	logger          *zap.Logger
	clusterMetadata *metadata.ClusterMetadata

	// This lock is used to protect the following field.
	lock      sync.RWMutex
	isRunning atomic.Bool
}

func NewNodeInspector(logger *zap.Logger, clusterMetadata *metadata.ClusterMetadata) *NodeInspector {
	return &NodeInspector{
		logger:          logger,
		clusterMetadata: clusterMetadata,
		lock:            sync.RWMutex{},
		isRunning:       atomic.Bool{},
	}
}

func (i *NodeInspector) Start(ctx context.Context) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.isRunning.Load() {
		return nil
	}

	log.Info("node inspector start")

	go func() {
		i.isRunning.Store(true)
		for {
			if !i.isRunning.Load() {
				i.logger.Info("scheduler manager is canceled")
				return
			}

			time.Sleep(inspectInterval)

			// Get latest cluster snapshot.
			snapshot := i.clusterMetadata.GetClusterSnapshot()
			i.inspect(ctx, snapshot)
		}
	}()

	return nil
}

func (i *NodeInspector) Stop(_ context.Context) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.isRunning.Load() {
		i.isRunning.Store(false)
	}

	return nil
}

func (i *NodeInspector) inspect(ctx context.Context, snapshot metadata.Snapshot) {
	shardNodeMapping := make(map[string][]storage.ShardNode, len(snapshot.RegisteredNodes))
	for _, shardNode := range snapshot.Topology.ClusterView.ShardNodes {
		if _, exists := shardNodeMapping[shardNode.NodeName]; !exists {
			shardNodeMapping[shardNode.NodeName] = []storage.ShardNode{}
		}
		shardNodeMapping[shardNode.NodeName] = append(shardNodeMapping[shardNode.NodeName], shardNode)
	}

	expiredShardNodes := make([]storage.ShardNode, 0, len(snapshot.RegisteredNodes))
	// Check node status.
	now := time.Now()
	for _, node := range snapshot.RegisteredNodes {
		if node.IsExpired(now) {
			expired, exists := shardNodeMapping[node.Node.Name]
			if !exists {
				log.Debug("try to remove non-existent node")
				continue
			}
			expiredShardNodes = append(expiredShardNodes, expired...)
		}
	}

	// Try to remove useless data if it exists.
	if err := i.clusterMetadata.DropShardNode(ctx, expiredShardNodes); err != nil {
		log.Error("drop shard node failed", zap.Error(err))
	}
}
