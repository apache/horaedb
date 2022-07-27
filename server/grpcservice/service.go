// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpcservice

import (
	"context"
	"io"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"go.uber.org/zap"
)

type Service struct {
	metaservicepb.UnimplementedCeresmetaRpcServiceServer

	opTimeout time.Duration
	h         Handler
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

	return nil
}

func (s *Service) NodeHeartbeat(heartbeatSrv metaservicepb.CeresmetaRpcService_NodeHeartbeatServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

		if err := binder.bindIfNot(ctx, req.Info.Node); err != nil {
			log.Error("fail to bind node stream", zap.Error(err))
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
