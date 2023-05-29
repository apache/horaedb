// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ddl

import (
	"context"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

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

func BuildCreateTableRequest(table storage.Table, shardVersionUpdate metadata.ShardVersionUpdate, req *metaservicepb.CreateTableRequest) eventdispatch.CreateTableOnShardRequest {
	return eventdispatch.CreateTableOnShardRequest{
		UpdateShardInfo: eventdispatch.UpdateShardInfo{
			CurrShardInfo: metadata.ShardInfo{
				ID: shardVersionUpdate.ShardID,
				// TODO: dispatch CreateTableOnShard to followers?
				Role:    storage.ShardRoleLeader,
				Version: shardVersionUpdate.CurrVersion,
			},
			PrevVersion: shardVersionUpdate.PrevVersion,
		},
		TableInfo: metadata.TableInfo{
			ID:            table.ID,
			Name:          table.Name,
			SchemaID:      table.SchemaID,
			SchemaName:    req.GetSchemaName(),
			PartitionInfo: table.PartitionInfo,
		},
		EncodedSchema:    req.EncodedSchema,
		Engine:           req.Engine,
		CreateIfNotExist: req.CreateIfNotExist,
		Options:          req.Options,
	}
}

func GetShardVersionByTableName(clusterMetadata *metadata.ClusterMetadata, schemaName, tableName string, shardVersions map[storage.ShardID]uint64) (storage.Table, metadata.ShardVersionUpdate, error) {
	table, exists, err := clusterMetadata.GetTable(schemaName, tableName)
	if err != nil {
		return storage.Table{}, metadata.ShardVersionUpdate{}, err
	}
	if !exists {
		return storage.Table{}, metadata.ShardVersionUpdate{}, errors.WithMessage(procedure.ErrTableNotExists, "table not exists")
	}

	shardNodesResult, err := clusterMetadata.GetShardNodeByTableIDs([]storage.TableID{table.ID})
	if err != nil {
		return storage.Table{}, metadata.ShardVersionUpdate{}, err
	}

	leader := storage.ShardNode{}
	found := false
	for _, shardNode := range shardNodesResult.ShardNodes[table.ID] {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			found = true
			leader = shardNode
			break
		}
	}

	if !found {
		return storage.Table{}, metadata.ShardVersionUpdate{}, errors.WithMessage(procedure.ErrShardLeaderNotFound, "can't find leader")
	}

	prevVersion, exists := shardVersions[leader.ID]
	if !exists {
		return storage.Table{}, metadata.ShardVersionUpdate{}, errors.WithMessagef(metadata.ErrShardNotFound, "shard not found in shardVersions, shardID:%d", leader.ID)
	}

	currVersion := prevVersion + 1

	return table, metadata.ShardVersionUpdate{
		ShardID:     leader.ID,
		CurrVersion: currVersion,
		PrevVersion: prevVersion,
	}, nil
}

func DispatchDropTable(ctx context.Context, clusterMetadata *metadata.ClusterMetadata, dispatch eventdispatch.Dispatch, schemaName string, table storage.Table, version metadata.ShardVersionUpdate) error {
	shardNodes, err := clusterMetadata.GetShardNodesByShardID(version.ShardID)
	if err != nil {
		return errors.WithMessage(err, "cluster get shard by shard id")
	}

	tableInfo := metadata.TableInfo{
		ID:         table.ID,
		Name:       table.Name,
		SchemaID:   table.SchemaID,
		SchemaName: schemaName,
	}

	for _, shardNode := range shardNodes {
		err = dispatch.DropTableOnShard(ctx, shardNode.NodeName, eventdispatch.DropTableOnShardRequest{
			UpdateShardInfo: eventdispatch.UpdateShardInfo{
				CurrShardInfo: metadata.ShardInfo{
					ID:      version.ShardID,
					Role:    storage.ShardRoleLeader,
					Version: version.CurrVersion,
				},
				PrevVersion: version.PrevVersion,
			},
			TableInfo: tableInfo,
		})
		if err != nil {
			return errors.WithMessage(err, "dispatch drop table on shard")
		}
	}

	return nil
}
