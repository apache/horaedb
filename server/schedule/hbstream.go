// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package schedule

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"go.uber.org/zap"
)

const (
	defaultHeartbeatMsgCap int = 1024
)

type sendReq struct {
	ctx  context.Context
	node string
	msg  *metaservicepb.NodeHeartbeatResponse
}

// HeartbeatStreams manages all the streams connected by ceresdb node.
type HeartbeatStreams struct {
	ctx     context.Context
	cancel  context.CancelFunc
	bgJobWg *sync.WaitGroup

	reqCh chan *sendReq

	// mu protects nodeStreams
	mu *sync.RWMutex
	// TODO: now these streams only can be removed by Unbind method and it is better add a active way to clear the dead
	//  streams in background.
	nodeStreams map[string]HeartbeatStreamSender
}

func NewHeartbeatStreams(ctx context.Context) *HeartbeatStreams {
	ctx, cancel := context.WithCancel(ctx)
	h := &HeartbeatStreams{
		ctx:     ctx,
		cancel:  cancel,
		bgJobWg: &sync.WaitGroup{},

		reqCh:       make(chan *sendReq, defaultHeartbeatMsgCap),
		mu:          &sync.RWMutex{},
		nodeStreams: make(map[string]HeartbeatStreamSender),
	}

	go h.runBgJob()

	return h
}

type HeartbeatStreamSender interface {
	Send(response *metaservicepb.NodeHeartbeatResponse) error
}

func (h *HeartbeatStreams) runBgJob() {
	h.bgJobWg.Add(1)
	defer h.bgJobWg.Done()

	for {
		select {
		case req := <-h.reqCh:
			err := h.sendMsgOnce(req)
			if err != nil {
				log.Error("fail to send msg", zap.Error(err), zap.String("node", req.node), zap.Any("msg", req.msg))
			}
		case <-h.ctx.Done():
			log.Warn("exit from background jobs")
			return
		}
	}
}

func (h *HeartbeatStreams) sendMsgOnce(hbMsg *sendReq) error {
	sender := h.getStream(hbMsg.node)
	if sender == nil {
		return ErrStreamNotAvailable
	}

	// TODO: set a timeout for sending messages
	if err := sender.Send(hbMsg.msg); err != nil {
		return ErrStreamSendMsg.WithCause(err)
	}

	return nil
}

// getStream finds and returns the sender bound to the node, and it is nil if not exist.
func (h *HeartbeatStreams) getStream(node string) HeartbeatStreamSender {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.nodeStreams[node]
}

func (h *HeartbeatStreams) Bind(node string, sender HeartbeatStreamSender) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.nodeStreams[node] = sender
}

func (h *HeartbeatStreams) Unbind(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.nodeStreams, node)
}

// SendMsgAsync sends messages to node and this procedure is asynchronous.
func (h *HeartbeatStreams) SendMsgAsync(ctx context.Context, node string, msg *metaservicepb.NodeHeartbeatResponse) error {
	req := &sendReq{
		ctx,
		node,
		msg,
	}

	select {
	case h.reqCh <- req:
		return nil
	case <-ctx.Done():
		return ErrStreamSendTimeout
	case <-h.ctx.Done():
		return ErrHeartbeatStreamsClosed
	}
}

// Close cancels and waits for all the waiting goroutines.
func (h *HeartbeatStreams) Close() {
	h.cancel()
	h.bgJobWg.Wait()
}
