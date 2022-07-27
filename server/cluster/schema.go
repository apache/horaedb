// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import "github.com/CeresDB/ceresdbproto/pkg/clusterpb"

type Schema struct {
	meta *clusterpb.Schema

	tableMap map[string]*Table
}

func (s *Schema) GetID() uint32 {
	return s.meta.GetId()
}

func (s *Schema) getTable(tableName string) (*Table, bool) {
	table, ok := s.tableMap[tableName]
	return table, ok
}

func (s *Schema) dropTableLocked(tableName string) {
	delete(s.tableMap, tableName)
}
