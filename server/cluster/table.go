// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import "github.com/CeresDB/ceresdbproto/pkg/clusterpb"

type Table struct {
	shardID uint32
	schema  *clusterpb.Schema
	meta    *clusterpb.Table
}

func (t *Table) GetInfo() TableInfo {
	return TableInfo{
		ID:         t.GetID(),
		Name:       t.GetName(),
		SchemaID:   t.GetSchemaID(),
		SchemaName: t.GetSchemaName(),
	}
}

func (t *Table) GetID() uint64 {
	return t.meta.GetId()
}

func (t *Table) GetName() string {
	return t.meta.GetName()
}

func (t *Table) GetSchemaName() string {
	return t.schema.GetName()
}

func (t *Table) GetSchemaID() uint32 {
	return t.schema.GetId()
}

func (t *Table) GetShardID() uint32 {
	return t.shardID
}
