// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpcservice

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// getForwardedCeresmetaClient get forwarded ceresmeta client. When current node is the leader, this func will return (nil,nil).
func (s *Service) getForwardedCeresmetaClient(ctx context.Context) (metaservicepb.CeresmetaRpcServiceClient, error) {
	forwardedAddr, _, err := s.getForwardedAddr(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get forwarded ceresmeta client")
	}

	if forwardedAddr != "" {
		ceresmetaClient, err := s.getCeresmetaClient(ctx, forwardedAddr)
		if err != nil {
			return nil, errors.Wrap(err, "get forwarded ceresmeta client")
		}
		return ceresmetaClient, nil
	}
	return nil, nil
}

func (s *Service) getCeresmetaClient(ctx context.Context, addr string) (metaservicepb.CeresmetaRpcServiceClient, error) {
	client, err := s.getForwardedGrpcClient(ctx, addr)
	if err != nil {
		return nil, errors.Wrapf(err, "get ceresmeta client, addr:%s", addr)
	}
	return metaservicepb.NewCeresmetaRpcServiceClient(client), nil
}

func (s *Service) getForwardedGrpcClient(ctx context.Context, forwardedAddr string) (*grpc.ClientConn, error) {
	client, ok := s.connConns.Load(forwardedAddr)
	if !ok {
		cc, err := getClientConn(ctx, forwardedAddr)
		if err != nil {
			return nil, err
		}
		client = cc
		s.connConns.Store(forwardedAddr, cc)
	}
	return client.(*grpc.ClientConn), nil
}

func (s *Service) getForwardedAddr(ctx context.Context) (string, bool, error) {
	member, err := s.h.GetLeader(ctx)
	if err != nil {
		return "", false, errors.Wrap(err, "get forwarded addr")
	}
	if member.IsLocal {
		return "", true, nil
	}
	return member.Leader.GetName(), false, nil
}

func (s *Service) createHeartbeatForwardedStream(ctx context.Context,
	client metaservicepb.CeresmetaRpcServiceClient,
) (metaservicepb.CeresmetaRpcService_NodeHeartbeatClient, error) {
	forwardedStream, err := client.NodeHeartbeat(ctx)
	return forwardedStream, err
}

func forwardRegionHeartbeatRespToClient(forwardedStream metaservicepb.CeresmetaRpcService_NodeHeartbeatClient,
	server metaservicepb.CeresmetaRpcService_NodeHeartbeatServer,
	errCh chan error,
) {
	defer close(errCh)
	for {
		resp, err := forwardedStream.Recv()
		if err != nil {
			errCh <- ErrForward.WithCause(err)
			return
		}
		if err := server.Send(resp); err != nil {
			errCh <- ErrForward.WithCause(err)
			return
		}
	}
}
