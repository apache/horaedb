// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ddl

import (
	"context"

	"github.com/CeresDB/ceresdbproto/golang/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func CreateTableMetadata(ctx context.Context, c *metadata.ClusterMetadata, schemaName string, tableName string, shardID storage.ShardID, partitionInfo *clusterpb.PartitionInfo) (metadata.CreateTableResult, error) {
	_, exists, err := c.GetTable(schemaName, tableName)
	if err != nil {
		return metadata.CreateTableResult{}, errors.WithMessage(err, "cluster get table")
	}
	if exists {
		return metadata.CreateTableResult{}, errors.WithMessagef(procedure.ErrTableAlreadyExists, "create an existing table, schemaName:%s, tableName:%s", schemaName, tableName)
	}

	createTableResult, err := c.CreateTable(ctx, metadata.CreateTableRequest{
		ShardID:       shardID,
		SchemaName:    schemaName,
		TableName:     tableName,
		PartitionInfo: storage.PartitionInfo{Info: partitionInfo},
	})
	if err != nil {
		return metadata.CreateTableResult{}, errors.WithMessage(err, "create table")
	}
	return createTableResult, nil
}

func CreateTableOnShard(ctx context.Context, c *metadata.ClusterMetadata, dispatch eventdispatch.Dispatch, shardID storage.ShardID, request eventdispatch.CreateTableOnShardRequest) error {
	log.Debug("CreateTableOnShard", zap.Uint64("curVersion", request.UpdateShardInfo.CurrShardInfo.Version), zap.Uint64("preVersion", request.UpdateShardInfo.PrevVersion))
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
		return errors.WithMessagef(procedure.ErrShardLeaderNotFound, "shard node can't find leader, shardID:%d", shardID)
	}

	err = dispatch.CreateTableOnShard(ctx, leader.NodeName, request)
	if err != nil {
		return errors.WithMessage(err, "create table on shard")
	}
	return nil
}

func BuildCreateTableRequest(createTableResult metadata.CreateTableResult, req *metaservicepb.CreateTableRequest, partitionInfo *clusterpb.PartitionInfo) eventdispatch.CreateTableOnShardRequest {
	return eventdispatch.CreateTableOnShardRequest{
		UpdateShardInfo: eventdispatch.UpdateShardInfo{
			CurrShardInfo: metadata.ShardInfo{
				ID: createTableResult.ShardVersionUpdate.ShardID,
				// TODO: dispatch CreateTableOnShard to followers?
				Role:    storage.ShardRoleLeader,
				Version: createTableResult.ShardVersionUpdate.CurrVersion,
			},
			PrevVersion: createTableResult.ShardVersionUpdate.PrevVersion,
		},
		TableInfo: metadata.TableInfo{
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
