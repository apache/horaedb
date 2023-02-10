// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
)

const (
	TestTableName0                         = "table0"
	TestTableName1                         = "table1"
	TestSchemaName                         = "TestSchemaName"
	NodeName0                              = "node0"
	NodeName1                              = "node1"
	TestRootPath                           = "/rootPath"
	DefaultIDAllocatorStep                 = 20
	ClusterName                            = "ceresdbCluster1"
	DefaultNodeCount                       = 2
	DefaultReplicationFactor               = 1
	DefaultPartitionTableProportionOfNodes = 0.5
	DefaultShardTotal                      = 4
)

type MockDispatch struct{}

func (m MockDispatch) OpenShard(_ context.Context, _ string, _ eventdispatch.OpenShardRequest) error {
	return nil
}

func (m MockDispatch) CloseShard(_ context.Context, _ string, _ eventdispatch.CloseShardRequest) error {
	return nil
}

func (m MockDispatch) CreateTableOnShard(_ context.Context, _ string, _ eventdispatch.CreateTableOnShardRequest) error {
	return nil
}

func (m MockDispatch) DropTableOnShard(_ context.Context, _ string, _ eventdispatch.DropTableOnShardRequest) error {
	return nil
}

func (m MockDispatch) CloseTableOnShard(_ context.Context, _ string, _ eventdispatch.CloseTableOnShardRequest) error {
	return nil
}

func (m MockDispatch) OpenTableOnShard(_ context.Context, _ string, _ eventdispatch.OpenTableOnShardRequest) error {
	return nil
}

type MockStorage struct{}

func (m MockStorage) CreateOrUpdate(_ context.Context, _ procedure.Meta) error {
	return nil
}

func (m MockStorage) List(_ context.Context, _ int) ([]*procedure.Meta, error) {
	return nil, nil
}

func (m MockStorage) MarkDeleted(_ context.Context, _ uint64) error {
	return nil
}

func NewTestStorage(_ *testing.T) procedure.Storage {
	return MockStorage{}
}
