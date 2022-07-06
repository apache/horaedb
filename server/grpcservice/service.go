// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpcservice

import (
	"context"
	"io"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"go.uber.org/zap"
)

type Service struct {
	metapb.UnimplementedCeresmetaRpcServiceServer

	opTimeout time.Duration
	h         Handler
}

func NewService(opTimeout time.Duration, h Handler) *Service {
	return &Service{
		opTimeout: opTimeout,
		h:         h,
	}
}

type HeartbeatSender interface {
	Send(response *metapb.NodeHeartbeatResponse) error
}

type Handler interface {
	BindHeartbeatStream(ctx context.Context, node string, sender HeartbeatSender) error
	ProcessHeartbeat(ctx context.Context, req *metapb.NodeHeartbeatRequest) error
}

func (s *Service) NodeHeartbeat(heartbeatSrv metapb.CeresmetaRpcService_NodeHeartbeatServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	isStreamBound := false
	bindStreamIfNot := func(ctx context.Context, node string) {
		if isStreamBound {
			return
		}

		ctx1, cancel := context.WithTimeout(ctx, s.opTimeout)
		defer cancel()

		if err := s.h.BindHeartbeatStream(ctx1, node, heartbeatSrv); err != nil {
			log.Error("fail to bind node stream", zap.String("node", node), zap.Error(err))
		} else {
			isStreamBound = true
		}
	}

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

		bindStreamIfNot(ctx, req.Info.Node)
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
