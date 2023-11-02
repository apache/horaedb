/*
 * Copyright 2022 The CeresDB Authors
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
		log.Info("try to create ceresmeta client", zap.String("addr", forwardedAddr))
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
	resp, err := s.h.GetLeader(ctx)
	if err != nil {
		return "", false, errors.WithMessage(err, "get forwarded addr")
	}
	if resp.IsLocal {
		return "", true, nil
	}
	return resp.LeaderEndpoint, false, nil
}
