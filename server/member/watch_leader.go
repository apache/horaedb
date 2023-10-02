// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package member

import (
	"context"
	"time"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metastoragepb"
	"github.com/CeresDB/ceresmeta/pkg/assert"
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

// LeaderWatcher watches the changes of the CeresMeta cluster's leadership.
type LeaderWatcher struct {
	watchCtx    WatchContext
	self        *Member
	leaseTTLSec int64

	leadershipChecker LeadershipChecker
}

type LeadershipEventCallbacks interface {
	AfterElected(ctx context.Context)
	BeforeTransfer(ctx context.Context)
}

// LeadershipChecker tells which member should campaign the CeresMeta cluster's leadership, and whether the current leader is valid.
type LeadershipChecker interface {
	ShouldCampaign(self *Member) bool
	IsValidLeader(memLeader *metastoragepb.Member) bool
}

// embeddedEtcdLeadershipChecker ensures the CeresMeta cluster's leader as the embedded ETCD cluster's leader.
type embeddedEtcdLeadershipChecker struct {
	etcdLeaderGetter etcdutil.EtcdLeaderGetter
}

func (c embeddedEtcdLeadershipChecker) ShouldCampaign(self *Member) bool {
	etcdLeaderID, err := c.etcdLeaderGetter.EtcdLeaderID()
	assert.Assertf(err == nil, "EtcdLeaderID must exist")
	return self.ID == etcdLeaderID
}

func (c embeddedEtcdLeadershipChecker) IsValidLeader(memLeader *metastoragepb.Member) bool {
	etcdLeaderID, err := c.etcdLeaderGetter.EtcdLeaderID()
	assert.Assertf(err == nil, "EtcdLeaderID must exist")
	return memLeader.Id == etcdLeaderID
}

// externalEtcdLeadershipChecker has no preference over the leadership of the CeresMeta cluster, that is to say, the leadership is random.
type externalEtcdLeadershipChecker struct{}

func (c externalEtcdLeadershipChecker) ShouldCampaign(_ *Member) bool {
	return true
}

func (c externalEtcdLeadershipChecker) IsValidLeader(_ *metastoragepb.Member) bool {
	return true
}

func NewLeaderWatcher(ctx WatchContext, self *Member, leaseTTLSec int64, embedEtcd bool) *LeaderWatcher {
	var leadershipChecker LeadershipChecker
	if embedEtcd {
		leadershipChecker = embeddedEtcdLeadershipChecker{
			etcdLeaderGetter: ctx,
		}
	} else {
		leadershipChecker = externalEtcdLeadershipChecker{}
	}

	return &LeaderWatcher{
		ctx,
		self,
		leaseTTLSec,
		leadershipChecker,
	}
}

// watchWithCheckEtcdLeader watches the leader changes:
//  1. Check whether the leader is valid if leader exists.
//     - Leader is valid: wait for the leader changes.
//     - Leader is not valid: reset the leader by the current leader.
//  2. Campaign the leadership if leader does not exist.
//     - Campaign the leader if this member should.
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
		resp, err := l.self.getLeader(ctx)
		if err != nil {
			logger.Error("fail to get leader", zap.Error(err))
			wait = waitReasonFailEtcd
			continue
		}

		memLeader := resp.Leader
		if memLeader == nil {
			// Leader does not exist.
			// A new leader should be elected and the etcd leader should be elected as the new leader.
			if l.leadershipChecker.ShouldCampaign(l.self) {
				// Campaign the leader and block until leader changes.
				if err := l.self.CampaignAndKeepLeader(ctx, l.leaseTTLSec, l.leadershipChecker, callbacks); err != nil {
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
			// Cache leader in memory.
			l.self.leader = memLeader
			log.Info("update leader cache", zap.String("endpoint", memLeader.Endpoint))

			// Leader does exist.
			// A new leader should be elected (the leader should be reset by the current leader itself) if the leader is
			// not the etcd leader.
			if l.leadershipChecker.IsValidLeader(memLeader) {
				// watch the leader and block until leader changes.
				l.self.WaitForLeaderChange(ctx, resp.Revision)
				logger.Warn("leader changes and stop watching")
				continue
			}

			// This leader is not valid, reset it if this member will campaign this leadership.
			if l.leadershipChecker.ShouldCampaign(l.self) {
				if err = l.self.ResetLeader(ctx); err != nil {
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
