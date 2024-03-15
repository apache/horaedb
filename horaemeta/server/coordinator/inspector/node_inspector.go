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
	"time"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"go.uber.org/zap"
)

var ErrStartAgain = coderr.NewCodeError(coderr.Internal, "try to start again")
var ErrStopNotStart = coderr.NewCodeError(coderr.Internal, "try to stop a not-started inspector")

const inspectInterval = time.Second * 5

// NodeInspector will inspect node status and remove expired data.
type NodeInspector struct {
	logger          *zap.Logger
	clusterMetadata ClusterMetaDataManipulator

	starter sync.Once
	// After `Start` is called, the following fields will be initialized
	stopCtx     context.Context
	bgJobCancel context.CancelFunc
}

// ClusterMetaDataManipulator provides the snapshot for NodeInspector to check and utilities of drop expired shard nodes.
type ClusterMetaDataManipulator interface {
	GetClusterSnapshot() metadata.Snapshot
	DropShardNode(context.Context, []storage.ShardNode) error
}

func NewNodeInspector(logger *zap.Logger, clusterMetadata ClusterMetaDataManipulator) *NodeInspector {
	return &NodeInspector{
		logger:          logger,
		clusterMetadata: clusterMetadata,
		starter:         sync.Once{},
		stopCtx:         nil,
		bgJobCancel:     nil,
	}
}

func (i *NodeInspector) Start(ctx context.Context) error {
	started := false
	i.starter.Do(func() {
		log.Info("node inspector start")
		started = true
		i.stopCtx, i.bgJobCancel = context.WithCancel(ctx)
		go func() {
			for {
				t := time.NewTimer(inspectInterval)
				select {
				case <-i.stopCtx.Done():
					i.logger.Info("node inspector is stopped, cancel the bg inspecting")
					if !t.Stop() {
						<-t.C
					}
					return
				case <-t.C:
				}

				i.inspect(ctx)
			}
		}()
	})

	if !started {
		return ErrStartAgain
	}

	return nil
}

func (i *NodeInspector) Stop(_ context.Context) error {
	if i.bgJobCancel != nil {
		i.bgJobCancel()
		return nil
	}

	return ErrStopNotStart
}

func (i *NodeInspector) inspect(ctx context.Context) {
	// Get latest cluster snapshot.
	snapshot := i.clusterMetadata.GetClusterSnapshot()
	expiredShardNodes := findExpiredShardNodes(snapshot)
	if len(expiredShardNodes) == 0 {
		return
	}

	// Try to remove useless data if it exists.
	if err := i.clusterMetadata.DropShardNode(ctx, expiredShardNodes); err != nil {
		log.Error("drop shard node failed", zap.Error(err))
	}
}

func findExpiredShardNodes(snapshot metadata.Snapshot) []storage.ShardNode {
	shardNodeMapping := make(map[string][]storage.ShardNode, len(snapshot.RegisteredNodes))
	for _, shardNode := range snapshot.Topology.ClusterView.ShardNodes {
		if _, exists := shardNodeMapping[shardNode.NodeName]; !exists {
			shardNodeMapping[shardNode.NodeName] = []storage.ShardNode{}
		}
		shardNodeMapping[shardNode.NodeName] = append(shardNodeMapping[shardNode.NodeName], shardNode)
	}

	// In most cases, there is no expired shard nodes so don't pre-allocate the memory.
	expiredShardNodes := make([]storage.ShardNode, 0)
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

	return expiredShardNodes
}
