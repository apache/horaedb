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

package coordinator

import (
	"context"

	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/storage"
)

type PersistShardPicker struct {
	cluster  *metadata.ClusterMetadata
	internal ShardPicker
}

func NewPersistShardPicker(cluster *metadata.ClusterMetadata, internal ShardPicker) *PersistShardPicker {
	return &PersistShardPicker{cluster: cluster, internal: internal}
}

func (p *PersistShardPicker) PickShards(ctx context.Context, snapshot metadata.Snapshot, schemaName string, tableNames []string) (map[string]storage.ShardNode, error) {
	result := map[string]storage.ShardNode{}

	shardNodeMap := make(map[storage.ShardID]storage.ShardNode, len(tableNames))
	for _, shardNode := range snapshot.Topology.ClusterView.ShardNodes {
		shardNodeMap[shardNode.ID] = shardNode
	}

	var missingTables []string
	// If table assign has been created, just reuse it.
	for i := 0; i < len(tableNames); i++ {
		shardID, exists, err := p.cluster.GetTableAssignedShard(ctx, schemaName, tableNames[i])
		if err != nil {
			return map[string]storage.ShardNode{}, err
		}
		if exists {
			result[tableNames[i]] = shardNodeMap[shardID]
		} else {
			missingTables = append(missingTables, tableNames[i])
		}
	}

	// All table has been assigned to shard.
	if len(missingTables) == 0 {
		return result, nil
	}

	// No table assign has been created, try to pick shard and save table assigns.
	shardNodes, err := p.internal.PickShards(ctx, snapshot, len(missingTables))
	if err != nil {
		return map[string]storage.ShardNode{}, err
	}

	for i, shardNode := range shardNodes {
		result[missingTables[i]] = shardNode
		err = p.cluster.AssignTableToShard(ctx, schemaName, missingTables[i], shardNode.ID)
		if err != nil {
			return map[string]storage.ShardNode{}, err
		}
	}

	return result, nil
}
