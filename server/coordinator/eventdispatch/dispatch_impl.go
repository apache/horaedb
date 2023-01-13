// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package eventdispatch

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaeventpb"
	"github.com/CeresDB/ceresmeta/pkg/coderr"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/service"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var ErrDispatch = coderr.NewCodeError(coderr.Internal, "event dispatch failed")

type DispatchImpl struct {
	conns sync.Map
}

func NewDispatchImpl() *DispatchImpl {
	return &DispatchImpl{}
}

func (d *DispatchImpl) OpenShard(ctx context.Context, addr string, request OpenShardRequest) error {
	client, err := d.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}
	resp, err := client.OpenShard(ctx, &metaeventpb.OpenShardRequest{
		Shard: cluster.ConvertShardsInfoToPB(request.Shard),
	})
	if err != nil {
		return errors.WithMessagef(err, "open shard, addr:%s, request:%v", addr, request)
	}
	if resp.GetHeader().Code != 0 {
		return ErrDispatch.WithCausef("open shard, addr:%s, request:%v, err:%s", addr, request, resp.GetHeader().GetError())
	}
	return nil
}

func (d *DispatchImpl) CloseShard(ctx context.Context, addr string, request CloseShardRequest) error {
	client, err := d.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}
	resp, err := client.CloseShard(ctx, &metaeventpb.CloseShardRequest{
		ShardId: request.ShardID,
	})
	if err != nil {
		return errors.WithMessagef(err, "close shard, addr:%s, request:%v", addr, request)
	}
	if resp.GetHeader().Code != 0 {
		return ErrDispatch.WithCausef("close shard, addr:%s, request:%v, err:%s", addr, request, resp.GetHeader().GetError())
	}
	return nil
}

func (d *DispatchImpl) CreateTableOnShard(ctx context.Context, addr string, request CreateTableOnShardRequest) error {
	client, err := d.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}
	resp, err := client.CreateTableOnShard(ctx, convertCreateTableOnShardRequestToPB(request))
	if err != nil {
		return errors.WithMessagef(err, "create table on shard, addr:%s, request:%v", addr, request)
	}
	if resp.GetHeader().Code != 0 {
		return ErrDispatch.WithCausef("create table on shard, addr:%s, request:%v, err:%s", addr, request, resp.GetHeader().GetError())
	}
	return nil
}

func (d *DispatchImpl) DropTableOnShard(ctx context.Context, addr string, request DropTableOnShardRequest) error {
	client, err := d.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}
	resp, err := client.DropTableOnShard(ctx, convertDropTableOnShardRequestToPB(request))
	if err != nil {
		return errors.WithMessagef(err, "drop table on shard, addr:%s, request:%v", addr, request)
	}
	if resp.GetHeader().Code != 0 {
		return ErrDispatch.WithCausef("drop table on shard, addr:%s, request:%v, err:%s", addr, request, resp.GetHeader().GetError())
	}
	return nil
}

func (d *DispatchImpl) OpenTableOnShard(ctx context.Context, addr string, request OpenTableOnShardRequest) error {
	client, err := d.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}

	resp, err := client.OpenTableOnShard(ctx, convertOpenTableOnShardRequestToPB(request))
	if err != nil {
		return errors.WithMessagef(err, "open table on shard, addr:%s, request:%v", addr, request)
	}
	if resp.GetHeader().Code != 0 {
		return ErrDispatch.WithCausef("open table on shard, addr:%s, request:%v, err:%s", addr, request, resp.GetHeader().GetError())
	}
	return nil
}

func (d *DispatchImpl) CloseTableOnShard(ctx context.Context, addr string, request CloseTableOnShardRequest) error {
	client, err := d.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}

	resp, err := client.CloseTableOnShard(ctx, convertCloseTableOnShardRequestToPB(request))
	if err != nil {
		return errors.WithMessagef(err, "close table on shard, addr:%s, request:%v", addr, request)
	}
	if resp.GetHeader().Code != 0 {
		return ErrDispatch.WithCausef("close table on shard, addr:%s, request:%v, err:%s", addr, request, resp.GetHeader().GetError())
	}
	return nil
}

func (d *DispatchImpl) getGrpcClient(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	client, ok := d.conns.Load(addr)
	if !ok {
		cc, err := service.GetClientConn(ctx, addr)
		if err != nil {
			return nil, err
		}
		client = cc
		d.conns.Store(addr, cc)
	}
	return client.(*grpc.ClientConn), nil
}

func (d *DispatchImpl) getMetaEventClient(ctx context.Context, addr string) (metaeventpb.MetaEventServiceClient, error) {
	client, err := d.getGrpcClient(ctx, addr)
	if err != nil {
		return nil, errors.WithMessagef(err, "get meta event client, addr:%s", addr)
	}
	return metaeventpb.NewMetaEventServiceClient(client), nil
}

func convertCreateTableOnShardRequestToPB(request CreateTableOnShardRequest) *metaeventpb.CreateTableOnShardRequest {
	return &metaeventpb.CreateTableOnShardRequest{
		UpdateShardInfo:      convertUpdateShardInfoToPB(request.UpdateShardInfo),
		TableInfo:            cluster.ConvertTableInfoToPB(request.TableInfo),
		EncodedSchema:        request.EncodedSchema,
		Engine:               request.Engine,
		CreateIfNotExist:     request.CreateIfNotExist,
		Options:              request.Options,
		EncodedPartitionInfo: request.EncodedPartitionInfo,
	}
}

func convertDropTableOnShardRequestToPB(request DropTableOnShardRequest) *metaeventpb.DropTableOnShardRequest {
	return &metaeventpb.DropTableOnShardRequest{
		UpdateShardInfo: convertUpdateShardInfoToPB(request.UpdateShardInfo),
		TableInfo:       cluster.ConvertTableInfoToPB(request.TableInfo),
	}
}

func convertCloseTableOnShardRequestToPB(request CloseTableOnShardRequest) *metaeventpb.CloseTableOnShardRequest {
	return &metaeventpb.CloseTableOnShardRequest{
		UpdateShardInfo: convertUpdateShardInfoToPB(request.UpdateShardInfo),
		TableInfo:       cluster.ConvertTableInfoToPB(request.TableInfo),
	}
}

func convertOpenTableOnShardRequestToPB(request OpenTableOnShardRequest) *metaeventpb.OpenTableOnShardRequest {
	return &metaeventpb.OpenTableOnShardRequest{
		UpdateShardInfo: convertUpdateShardInfoToPB(request.UpdateShardInfo),
		TableInfo:       cluster.ConvertTableInfoToPB(request.TableInfo),
	}
}

func convertUpdateShardInfoToPB(updateShardInfo UpdateShardInfo) *metaeventpb.UpdateShardInfo {
	return &metaeventpb.UpdateShardInfo{
		CurrShardInfo: cluster.ConvertShardsInfoToPB(updateShardInfo.CurrShardInfo),
		PrevVersion:   updateShardInfo.PrevVersion,
	}
}
