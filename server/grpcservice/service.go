// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpcservice

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/member"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Service struct {
	metaservicepb.UnimplementedCeresmetaRpcServiceServer
	opTimeout time.Duration
	h         Handler

	// Store as map[string]*grpc.ClientConn
	// TODO: remove unavailable connection
	connConns sync.Map
}

func NewService(opTimeout time.Duration, h Handler) *Service {
	return &Service{
		opTimeout: opTimeout,
		h:         h,
	}
}

type HeartbeatStreamSender interface {
	Send(response *metaservicepb.NodeHeartbeatResponse) error
}

// Handler is needed by grpc service to process the requests.
type Handler interface {
	GetClusterManager() cluster.Manager
	GetLeader(ctx context.Context) (*member.GetLeaderResp, error)
	UnbindHeartbeatStream(ctx context.Context, node string) error
	BindHeartbeatStream(ctx context.Context, node string, sender HeartbeatStreamSender) error
	ProcessHeartbeat(ctx context.Context, req *metaservicepb.NodeHeartbeatRequest) error

	// TODO: define the methods for handling other grpc requests.
}

type streamBinder struct {
	timeout time.Duration
	h       Handler
	stream  HeartbeatStreamSender

	// States of the binder which may be updated.
	node  string
	bound bool
}

func (b *streamBinder) bindIfNot(ctx context.Context, node string) error {
	if b.bound {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	if err := b.h.BindHeartbeatStream(ctx, node, b.stream); err != nil {
		return ErrBindHeartbeatStream.WithCausef("node:%s, err:%v", node, err)
	}

	log.Info("bind stream", zap.String("endpoint", node))

	b.bound = true
	b.node = node
	return nil
}

func (b *streamBinder) unbind(ctx context.Context) error {
	if !b.bound {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	if err := b.h.UnbindHeartbeatStream(ctx, b.node); err != nil {
		return ErrUnbindHeartbeatStream.WithCausef("node:%s, err:%v", b.node, err)
	}
	log.Info("unbind stream", zap.String("endpoint", b.node))
	return nil
}

// NodeHeartbeat implements gRPC CeresmetaServer.
func (s *Service) NodeHeartbeat(heartbeatSrv metaservicepb.CeresmetaRpcService_NodeHeartbeatServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := &forwarder{s: s, heartbeatSrv: heartbeatSrv, errCh: make(chan error, 1)}

	binder := streamBinder{
		timeout: s.opTimeout,
		h:       s.h,
		stream:  heartbeatSrv,
	}

	defer func() {
		if err := binder.unbind(ctx); err != nil {
			log.Error("fail to unbind stream", zap.Error(err))
		}
	}()

	// Process the message from the stream sequentially.
	for {
		req, err := heartbeatSrv.Recv()
		if err == io.EOF {
			log.Warn("receive EOF and exit the heartbeat loop")
			return nil
		}
		if err != nil {
			return ErrRecvHeartbeat.WithCause(err)
		}

		// Forward request to the leader.
		{
			err = f.maybeInitForwardedStream(ctx)
			if err != nil {
				return errors.Wrap(err, "node heartbeat")
			}

			if f.stream != nil {
				if err = f.stream.Send(req); err != nil {
					return errors.Wrap(err, "node heartbeat")
				}

				select {
				case err = <-f.errCh:
					log.Error("stream send failed", zap.String("endpoint", f.addr), zap.Error(err))
					return err
				default:
				}
				continue
			}
		}

		if err := binder.bindIfNot(ctx, req.Info.GetEndpoint()); err != nil {
			log.Error("fail to bind node stream", zap.String("endpoint", req.Info.GetEndpoint()), zap.Error(err))
		}

		func() {
			ctx1, cancel := context.WithTimeout(ctx, s.opTimeout)
			defer cancel()
			err := s.h.ProcessHeartbeat(ctx1, req)
			if err != nil {
				log.Error("fail to handle heartbeat", zap.Any("heartbeat", req), zap.Error(err))
			} else {
				log.Debug("succeed in handling heartbeat", zap.Any("heartbeat", req))
			}
		}()
	}
}

// AllocSchemaID implements gRPC CeresmetaServer.
func (s *Service) AllocSchemaID(ctx context.Context, req *metaservicepb.AllocSchemaIdRequest) (*metaservicepb.AllocSchemaIdResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.AllocSchemaIdResponse{Header: responseHeader(err, "grpc alloc schema id")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.AllocSchemaID(ctx, req)
	}

	schemaID, err := s.h.GetClusterManager().AllocSchemaID(ctx, req.GetHeader().GetClusterName(), req.GetName())
	if err != nil {
		return &metaservicepb.AllocSchemaIdResponse{Header: responseHeader(err, "grpc alloc schema id")}, nil
	}

	return &metaservicepb.AllocSchemaIdResponse{
		Header: okResponseHeader(),
		Name:   req.GetName(),
		Id:     schemaID,
	}, nil
}

// AllocTableID implements gRPC CeresmetaServer.
func (s *Service) AllocTableID(ctx context.Context, req *metaservicepb.AllocTableIdRequest) (*metaservicepb.AllocTableIdResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.AllocTableIdResponse{Header: responseHeader(err, "grpc alloc table id")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.AllocTableID(ctx, req)
	}

	table, err := s.h.GetClusterManager().AllocTableID(ctx, req.GetHeader().GetClusterName(), req.GetSchemaName(), req.GetName(), req.GetHeader().GetNode())
	if err != nil {
		return &metaservicepb.AllocTableIdResponse{Header: responseHeader(err, "grpc alloc table id")}, nil
	}

	return &metaservicepb.AllocTableIdResponse{
		Header:     okResponseHeader(),
		ShardId:    table.GetShardID(),
		SchemaName: table.GetSchemaName(),
		SchemaId:   table.GetSchemaID(),
		Name:       table.GetName(),
		Id:         table.GetID(),
	}, nil
}

// GetTables implements gRPC CeresmetaServer.
func (s *Service) GetTables(ctx context.Context, req *metaservicepb.GetShardTablesRequest) (*metaservicepb.GetShardTablesResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.GetShardTablesResponse{Header: responseHeader(err, "grpc get tables")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.GetTables(ctx, req)
	}

	tables, err := s.h.GetClusterManager().GetTables(ctx, req.GetHeader().GetClusterName(), req.GetHeader().GetNode(), req.GetShardIds())
	if err != nil {
		return &metaservicepb.GetShardTablesResponse{Header: responseHeader(err, "grpc get tables")}, nil
	}

	tablesPb := make(map[uint32]*metaservicepb.ShardTables, len(tables))
	for shardID, shardTables := range tables {
		tablesOfShardPb := make([]*metaservicepb.TableInfo, 0, len(shardTables.Tables))
		for _, table := range shardTables.Tables {
			tablesOfShardPb = append(tablesOfShardPb, &metaservicepb.TableInfo{
				Id:         table.ID,
				Name:       table.Name,
				SchemaId:   table.SchemaID,
				SchemaName: table.SchemaName,
			})
		}
		tablesPb[shardID] = &metaservicepb.ShardTables{
			Role:   shardTables.ShardRole,
			Tables: tablesOfShardPb,
		}
	}

	return &metaservicepb.GetShardTablesResponse{
		Header:      okResponseHeader(),
		ShardTables: tablesPb,
	}, nil
}

// DropTable implements gRPC CeresmetaServer.
func (s *Service) DropTable(ctx context.Context, req *metaservicepb.DropTableRequest) (*metaservicepb.DropTableResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.DropTableResponse{Header: responseHeader(err, "grpc drop table")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.DropTable(ctx, req)
	}

	err = s.h.GetClusterManager().DropTable(ctx, req.GetHeader().GetClusterName(), req.GetSchemaName(), req.GetName(), req.GetId())
	if err != nil {
		return &metaservicepb.DropTableResponse{Header: responseHeader(err, "grpc drop table")}, nil
	}

	return &metaservicepb.DropTableResponse{
		Header: okResponseHeader(),
	}, nil
}

// RouteTables implements gRPC CeresmetaServer.
func (s *Service) RouteTables(ctx context.Context, req *metaservicepb.RouteTablesRequest) (*metaservicepb.RouteTablesResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.RouteTablesResponse{Header: responseHeader(err, "grpc routeTables")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.RouteTables(ctx, req)
	}

	routeTableResult, err := s.h.GetClusterManager().RouteTables(ctx, req.GetHeader().GetClusterName(), req.GetSchemaName(), req.GetTableNames())
	if err != nil {
		return &metaservicepb.RouteTablesResponse{Header: responseHeader(err, "grpc routeTables")}, nil
	}

	return convertRouteTableResult(routeTableResult), nil
}

type forwarder struct {
	heartbeatSrv metaservicepb.CeresmetaRpcService_NodeHeartbeatServer
	s            *Service
	stream       metaservicepb.CeresmetaRpcService_NodeHeartbeatClient
	addr         string
	cancel       context.CancelFunc
	errCh        chan error
}

func (f *forwarder) needReconnect(addr string) bool {
	return f.stream == nil || addr != f.addr
}

func (f *forwarder) reconnect(addr string) error {
	dialCtx, dialCancel := context.WithTimeout(context.Background(), f.s.opTimeout)
	defer dialCancel()

	ceresmetaClient, err := f.s.getCeresmetaClient(dialCtx, addr)
	if err != nil {
		return errors.Wrap(err, "forwarder reconnect")
	}

	if ceresmetaClient != nil {
		ctx, cancel := context.WithCancel(context.Background())
		f.cancel = cancel
		f.stream, err = f.s.createHeartbeatForwardedStream(ctx, ceresmetaClient)
		if err != nil {
			cancel()
			return errors.Wrap(err, "forwarder reconnect")
		}

		// Forward the leader's response to the client.
		go forwardRegionHeartbeatRespToClient(f.stream, f.heartbeatSrv, f.errCh)
		return nil
	}
	return nil
}

func (f *forwarder) maybeInitForwardedStream(ctx context.Context) error {
	forwardedAddr, isLocal, err := f.s.getForwardedAddr(ctx)
	if err != nil {
		return err
	}

	// In following two cases, will reconnect:
	// 1. request need to be forwarded, but forwarded stream is nil.
	// 2. the forwarded addr(the leader addr) is changed, need to close previous forwarded stream and create new forwarded stream.
	if f.needReconnect(forwardedAddr) {
		// Close previous forwarded stream.
		if f.cancel != nil {
			f.cancel()
		}

		// When isLocal is false, request need to be forwarded to the leader.
		if !isLocal {
			log.Info("forwarder needReconnect", zap.String("addr", forwardedAddr), zap.String("previous", f.addr))
			if err = f.reconnect(forwardedAddr); err != nil {
				return err
			}
			f.addr = forwardedAddr
		}
	}
	return nil
}

func convertRouteTableResult(routeTablesResult *cluster.RouteTablesResult) *metaservicepb.RouteTablesResponse {
	entries := make(map[string]*metaservicepb.RouteEntry, len(routeTablesResult.RouteEntries))
	for tableName, entry := range routeTablesResult.RouteEntries {
		nodeShards := make([]*metaservicepb.NodeShard, 0, len(entry.NodeShards))
		for _, nodeShard := range entry.NodeShards {
			nodeShards = append(nodeShards, &metaservicepb.NodeShard{
				Endpoint: nodeShard.Endpoint,
				ShardInfo: &metaservicepb.ShardInfo{
					ShardId: nodeShard.ShardInfo.ShardID,
					Role:    nodeShard.ShardInfo.ShardRole,
				},
			})
		}

		entries[tableName] = &metaservicepb.RouteEntry{
			Table: &metaservicepb.TableInfo{
				Id:         entry.Table.ID,
				Name:       entry.Table.Name,
				SchemaId:   entry.Table.SchemaID,
				SchemaName: entry.Table.SchemaName,
			},
			NodeShards: nodeShards,
		}
	}

	return &metaservicepb.RouteTablesResponse{
		Header:                 okResponseHeader(),
		ClusterTopologyVersion: routeTablesResult.Version,
		Entries:                entries,
	}
}
