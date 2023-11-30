/*
 * Copyright 2022 The HoraeDB Authors
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

package storage

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Storage defines the storage operations on the ceresdb cluster meta info.
type Storage interface {
	// GetCluster get cluster metadata by clusterID.
	GetCluster(ctx context.Context, clusterID ClusterID) (Cluster, error)
	// ListClusters list all clusters.
	ListClusters(ctx context.Context) (ListClustersResult, error)
	// CreateCluster create new cluster, return error if cluster already exists.
	CreateCluster(ctx context.Context, req CreateClusterRequest) error
	// UpdateCluster update cluster metadata.
	UpdateCluster(ctx context.Context, req UpdateClusterRequest) error

	// CreateClusterView create cluster view.
	CreateClusterView(ctx context.Context, req CreateClusterViewRequest) error
	// GetClusterView get cluster view by cluster id.
	GetClusterView(ctx context.Context, req GetClusterViewRequest) (GetClusterViewResult, error)
	// UpdateClusterView update cluster view.
	UpdateClusterView(ctx context.Context, req UpdateClusterViewRequest) error

	// ListSchemas list all schemas in specified cluster.
	ListSchemas(ctx context.Context, req ListSchemasRequest) (ListSchemasResult, error)
	// CreateSchema create schema in specified cluster.
	CreateSchema(ctx context.Context, req CreateSchemaRequest) error

	// CreateTable create new table in specified cluster and schema, return error if table already exists.
	CreateTable(ctx context.Context, req CreateTableRequest) error
	// GetTable get table by table name in specified cluster and schema.
	GetTable(ctx context.Context, req GetTableRequest) (GetTableResult, error)
	// ListTables list all tables in specified cluster and schema.
	ListTables(ctx context.Context, req ListTableRequest) (ListTablesResult, error)
	// DeleteTable delete table by table name in specified cluster and schema.
	DeleteTable(ctx context.Context, req DeleteTableRequest) error

	// CreateShardViews create shard views in specified cluster.
	CreateShardViews(ctx context.Context, req CreateShardViewsRequest) error
	// ListShardViews list all shard views in specified cluster.
	ListShardViews(ctx context.Context, req ListShardViewsRequest) (ListShardViewsResult, error)
	// UpdateShardView update shard views in specified cluster.
	UpdateShardView(ctx context.Context, req UpdateShardViewRequest) error

	// ListNodes list all nodes in specified cluster.
	ListNodes(ctx context.Context, req ListNodesRequest) (ListNodesResult, error)
	// CreateOrUpdateNode create or update node in specified cluster.
	CreateOrUpdateNode(ctx context.Context, req CreateOrUpdateNodeRequest) error
}

// NewStorageWithEtcdBackend creates a new storage with etcd backend.
func NewStorageWithEtcdBackend(client *clientv3.Client, rootPath string, opts Options) Storage {
	return newEtcdStorage(client, rootPath, opts)
}
