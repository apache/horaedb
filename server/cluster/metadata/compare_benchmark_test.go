// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package metadata

import (
	"testing"

	"github.com/CeresDB/ceresmeta/server/storage"
)

func buildRegisterNode(shardNumber int) RegisteredNode {
	shardInfos := make([]ShardInfo, 0, shardNumber)
	for i := shardNumber; i > 0; i-- {
		shardInfos = append(shardInfos, ShardInfo{
			ID:      storage.ShardID(i),
			Role:    0,
			Version: 0,
		})
	}
	return RegisteredNode{ShardInfos: shardInfos}
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
