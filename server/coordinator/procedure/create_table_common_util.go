// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"

	"github.com/CeresDB/ceresdbproto/golang/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

func CreateTableMetadata(ctx context.Context, c *cluster.Cluster, schemaName string, tableName string, shardID storage.ShardID, partitionInfo *clusterpb.PartitionInfo) (cluster.CreateTableResult, error) {
	_, exists, err := c.GetTable(schemaName, tableName)
	if err != nil {
		return cluster.CreateTableResult{}, errors.WithMessage(err, "cluster get table")
	}
	if exists {
		return cluster.CreateTableResult{}, errors.WithMessagef(ErrTableAlreadyExists, "create an existing table, schemaName:%s, tableName:%s", schemaName, tableName)
	}

	createTableResult, err := c.CreateTable(ctx, cluster.CreateTableRequest{
		ShardID:       shardID,
		SchemaName:    schemaName,
		TableName:     tableName,
		PartitionInfo: storage.PartitionInfo{Info: partitionInfo},
	})
	if err != nil {
		return cluster.CreateTableResult{}, errors.WithMessage(err, "create table")
	}
	return createTableResult, nil
}

func CreateTableOnShard(ctx context.Context, c *cluster.Cluster, dispatch eventdispatch.Dispatch, shardID storage.ShardID, request eventdispatch.CreateTableOnShardRequest) error {
	shardNodes, err := c.GetShardNodesByShardID(shardID)
	if err != nil {
		return errors.WithMessage(err, "cluster get shardNode by id")
	}
	// TODO: consider followers
	leader := storage.ShardNode{}
	found := false
	for _, shardNode := range shardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			found = true
			leader = shardNode
			break
		}
	}
	if !found {
		return errors.WithMessagef(ErrShardLeaderNotFound, "shard node can't find leader, shardID:%d", shardID)
	}

	err = dispatch.CreateTableOnShard(ctx, leader.NodeName, request)
	if err != nil {
		return errors.WithMessage(err, "create table on shard")
	}
	return nil
}

func BuildCreateTableRequest(createTableResult cluster.CreateTableResult, req *metaservicepb.CreateTableRequest, partitionInfo *clusterpb.PartitionInfo) eventdispatch.CreateTableOnShardRequest {
	return eventdispatch.CreateTableOnShardRequest{
		UpdateShardInfo: eventdispatch.UpdateShardInfo{
			CurrShardInfo: cluster.ShardInfo{
				ID: createTableResult.ShardVersionUpdate.ShardID,
				// TODO: dispatch CreateTableOnShard to followers?
				Role:    storage.ShardRoleLeader,
				Version: createTableResult.ShardVersionUpdate.CurrVersion,
			},
			PrevVersion: createTableResult.ShardVersionUpdate.PrevVersion,
		},
		TableInfo: cluster.TableInfo{
			ID:            createTableResult.Table.ID,
			Name:          createTableResult.Table.Name,
			SchemaID:      createTableResult.Table.SchemaID,
			SchemaName:    req.GetSchemaName(),
			PartitionInfo: storage.PartitionInfo{Info: partitionInfo},
		},
		EncodedSchema:    req.EncodedSchema,
		Engine:           req.Engine,
		CreateIfNotExist: req.CreateIfNotExist,
		Options:          req.Options,
	}
}
