// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import "github.com/CeresDB/ceresdbproto/pkg/clusterpb"

type Shard struct {
	meta    []*clusterpb.Shard
	nodes   []*clusterpb.Node
	tables  map[uint64]*Table // table_id => table
	version uint64
}

func (s *Shard) dropTableLocked(tableID uint64) {
	delete(s.tables, tableID)
}

type ShardTablesWithRole struct {
	shardRole clusterpb.ShardRole
	tables    []*Table
	version   uint64
}
