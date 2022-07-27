// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import "github.com/CeresDB/ceresdbproto/pkg/clusterpb"

type Table struct {
	schema *clusterpb.Schema
	meta   *clusterpb.Table
}

func (t *Table) GetID() uint64 {
	return t.meta.GetId()
}
