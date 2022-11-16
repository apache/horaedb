// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpc

import (
	"context"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/service"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// getForwardedCeresmetaClient get forwarded ceresmeta client. When current node is the leader, this func will return (nil,nil).
func (s *Service) getForwardedCeresmetaClient(ctx context.Context) (metaservicepb.CeresmetaRpcServiceClient, error) {
	forwardedAddr, _, err := s.getForwardedAddr(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "get forwarded ceresmeta client")
	}

	if forwardedAddr != "" {
		log.Info("try to create ceresmeta client", zap.String("addr", forwardedAddr))
		ceresmetaClient, err := s.getCeresmetaClient(ctx, forwardedAddr)
		if err != nil {
			return nil, errors.WithMessagef(err, "get forwarded ceresmeta client, addr:%s", forwardedAddr)
		}
		return ceresmetaClient, nil
	}
	return nil, nil
}

func (s *Service) getCeresmetaClient(ctx context.Context, addr string) (metaservicepb.CeresmetaRpcServiceClient, error) {
	client, err := s.getForwardedGrpcClient(ctx, addr)
	if err != nil {
		return nil, errors.WithMessagef(err, "get ceresmeta client, addr:%s", addr)
	}
	return metaservicepb.NewCeresmetaRpcServiceClient(client), nil
}

func (s *Service) getForwardedGrpcClient(ctx context.Context, forwardedAddr string) (*grpc.ClientConn, error) {
	client, ok := s.conns.Load(forwardedAddr)
	if !ok {
		cc, err := service.GetClientConn(ctx, forwardedAddr)
		if err != nil {
			return nil, err
		}
		client = cc
		s.conns.Store(forwardedAddr, cc)
	}
	return client.(*grpc.ClientConn), nil
}

func (s *Service) getForwardedAddr(ctx context.Context) (string, bool, error) {
	member, err := s.h.GetLeader(ctx)
	if err != nil {
		return "", false, errors.WithMessage(err, "get forwarded addr")
	}
	if member.IsLocal {
		return "", true, nil
	}
	return member.Leader.GetEndpoint(), false, nil
}
