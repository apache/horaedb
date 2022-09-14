// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package schedule

import (
	"context"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/pkg/errors"
)

type Event interface {
	ToHeartbeatResp() *metaservicepb.NodeHeartbeatResponse
}

type NoneEvent struct{}

func (*NoneEvent) ToHeartbeatResp() *metaservicepb.NodeHeartbeatResponse {
	return &metaservicepb.NodeHeartbeatResponse{Timestamp: uint64(time.Now().UnixMilli())}
}

type OpenEvent struct {
	ShardIDs []uint32
}

func (open *OpenEvent) ToHeartbeatResp() *metaservicepb.NodeHeartbeatResponse {
	return &metaservicepb.NodeHeartbeatResponse{
		Timestamp: uint64(time.Now().UnixMilli()),
		Cmd: &metaservicepb.NodeHeartbeatResponse_OpenCmd{
			OpenCmd: &metaservicepb.OpenCmd{
				ShardIds: open.ShardIDs,
			},
		},
	}
}

type EventHandler struct {
	hbstreams *HeartbeatStreams
}

func NewEventHandler(hbstream *HeartbeatStreams) *EventHandler {
	return &EventHandler{
		hbstreams: hbstream,
	}
}

func (e *EventHandler) Dispatch(ctx context.Context, nodeName string, event Event) error {
	if err := e.hbstreams.SendMsgAsync(ctx, nodeName, event.ToHeartbeatResp()); err != nil {
		return errors.WithMessage(err, "event handler dispatch")
	}
	return nil
}
