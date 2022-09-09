// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package member

import (
	"context"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"go.uber.org/zap"
)

const (
	watchLeaderFailInterval = time.Duration(200) * time.Millisecond

	waitReasonFailEtcd    = "fail to access etcd"
	waitReasonResetLeader = "leader is reset"
	waitReasonElectLeader = "leader is electing"
	waitReasonNoWait      = ""
)

type WatchContext interface {
	etcdutil.EtcdLeaderGetter
	ShouldStop() bool
}

type LeaderWatcher struct {
	watchCtx    WatchContext
	self        *Member
	leaseTTLSec int64
}

type LeadershipEventCallbacks interface {
	AfterElected(ctx context.Context)
	BeforeTransfer(ctx context.Context)
}

func NewLeaderWatcher(ctx WatchContext, self *Member, leaseTTLSec int64) *LeaderWatcher {
	return &LeaderWatcher{
		ctx,
		self,
		leaseTTLSec,
	}
}

// Watch watches the leader changes:
//  1. Check whether the leader is valid (same as the etcd leader) if leader exists.
//     - Leader is valid: wait for the leader changes.
//     - Leader is not valid: reset the leader by the current leader.
//  2. Campaign the leadership if leader does not exist.
//     - Elect the etcd leader as the ceresmeta leader.
//     - The leader keeps the leadership lease alive.
//     - The other members keeps waiting for the leader changes.
//
// The LeadershipCallbacks `callbacks` will be triggered when specific events occur.
func (l *LeaderWatcher) Watch(ctx context.Context, callbacks LeadershipEventCallbacks) {
	var wait string
	logger := log.With(zap.String("self", l.self.Name))

	for {
		if l.watchCtx.ShouldStop() {
			logger.Warn("stop watching leader because of server is closed")
			return
		}

		select {
		case <-ctx.Done():
			logger.Warn("stop watching leader because ctx is done")
			return
		default:
		}

		if wait != waitReasonNoWait {
			logger.Warn("sleep a while during watch", zap.String("wait-reason", wait))
			time.Sleep(watchLeaderFailInterval)
			wait = waitReasonNoWait
		}

		// Check whether leader exists.
		leaderResp, err := l.self.GetLeader(ctx)
		if err != nil {
			logger.Error("fail to get leader", zap.Error(err))
			wait = waitReasonFailEtcd
			continue
		}

		etcdLeaderID := l.watchCtx.EtcdLeaderID()
		if leaderResp.Leader == nil {
			// Leader does not exist.
			// A new leader should be elected and the etcd leader should be elected as the new leader.
			if l.self.ID == etcdLeaderID {
				// Campaign the leader and block until leader changes.
				if err := l.self.CampaignAndKeepLeader(ctx, l.leaseTTLSec, callbacks); err != nil {
					logger.Error("fail to campaign and keep leader", zap.Error(err))
					wait = waitReasonFailEtcd
				} else {
					logger.Info("stop keeping leader")
				}
				continue
			}

			// For other nodes that is not etcd leader, just wait for the new leader elected.
			wait = waitReasonElectLeader
		} else {
			// Leader does exist.
			// A new leader should be elected (the leader should be reset by the current leader itself) if the leader is
			// not the etcd leader.
			if etcdLeaderID == leaderResp.Leader.Id {
				// watch the leader and block until leader changes.
				l.self.WaitForLeaderChange(ctx, leaderResp.Revision)
				logger.Warn("leader changes and stop watching")
				continue
			}

			// The leader is not etcd leader and this node is leader so reset it.
			if leaderResp.Leader.Id == l.self.ID {
				if err := l.self.ResetLeader(ctx); err != nil {
					logger.Error("fail to reset leader", zap.Error(err))
					wait = waitReasonFailEtcd
				}
				continue
			}

			// The leader is not etcd leader and this node is not the leader so just wait a moment and check leader again.
			wait = waitReasonResetLeader
		}
	}
}
