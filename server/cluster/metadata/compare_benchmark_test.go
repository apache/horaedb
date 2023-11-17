/*
 * Copyright 2022 The CeresDB Authors
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

package metadata

import (
	"testing"

	"github.com/CeresDB/horaemeta/server/storage"
)

func buildRegisterNode(shardNumber int) RegisteredNode {
	shardInfos := make([]ShardInfo, 0, shardNumber)
	for i := shardNumber; i > 0; i-- {
		shardInfos = append(shardInfos, ShardInfo{
			ID:      storage.ShardID(i),
			Role:    0,
			Version: 0,
			Status:  storage.ShardStatusUnknown,
		})
	}
	return RegisteredNode{
		Node: storage.Node{
			Name:          "",
			NodeStats:     storage.NewEmptyNodeStats(),
			LastTouchTime: 0,
			State:         storage.NodeStateUnknown,
		},
		ShardInfos: shardInfos,
	}
}

func BenchmarkSortWith10Shards(b *testing.B) {
	registerNode := buildRegisterNode(10)
	oldCache := registerNode
	for i := 0; i < b.N; i++ {
		sortCompare(oldCache.ShardInfos, registerNode.ShardInfos)
	}
}

func BenchmarkCompareWith10Shards(b *testing.B) {
	registerNode := buildRegisterNode(10)
	oldCache := registerNode
	for i := 0; i < b.N; i++ {
		simpleCompare(oldCache.ShardInfos, registerNode.ShardInfos)
	}
}

func BenchmarkSortWith50Shards(b *testing.B) {
	registerNode := buildRegisterNode(50)
	oldCache := registerNode
	for i := 0; i < b.N; i++ {
		sortCompare(oldCache.ShardInfos, registerNode.ShardInfos)
	}
}

func BenchmarkCompareWith50Shards(b *testing.B) {
	registerNode := buildRegisterNode(50)
	oldCache := registerNode
	for i := 0; i < b.N; i++ {
		simpleCompare(oldCache.ShardInfos, registerNode.ShardInfos)
	}
}

func BenchmarkSortWith100Shards(b *testing.B) {
	registerNode := buildRegisterNode(100)
	oldCache := registerNode
	for i := 0; i < b.N; i++ {
		sortCompare(oldCache.ShardInfos, registerNode.ShardInfos)
	}
}

func BenchmarkCompareWith100Shards(b *testing.B) {
	registerNode := buildRegisterNode(100)
	oldCache := registerNode
	for i := 0; i < b.N; i++ {
		simpleCompare(oldCache.ShardInfos, registerNode.ShardInfos)
	}
}
