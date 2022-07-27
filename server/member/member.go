// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package member

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/metastoragepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const leaderCheckInterval = time.Duration(100) * time.Millisecond

// Member manages the leadership and the role of the node in the ceresmeta cluster.
type Member struct {
	ID               uint64
	Name             string
	rootPath         string
	leaderKey        string
	etcdCli          *clientv3.Client
	etcdLeaderGetter etcdutil.EtcdLeaderGetter
	leader           *metastoragepb.Member
	rpcTimeout       time.Duration
	logger           *zap.Logger
}

func formatLeaderKey(rootPath string) string {
	return fmt.Sprintf("%s/members/leader", rootPath)
}

func NewMember(rootPath string, id uint64, name string, etcdCli *clientv3.Client, etcdLeaderGetter etcdutil.EtcdLeaderGetter, rpcTimeout time.Duration) *Member {
	leaderKey := formatLeaderKey(rootPath)
	logger := log.With(zap.String("node-name", name), zap.Uint64("node-id", id))
	return &Member{
		ID:               id,
		Name:             name,
		rootPath:         rootPath,
		leaderKey:        leaderKey,
		etcdCli:          etcdCli,
		etcdLeaderGetter: etcdLeaderGetter,
		leader:           nil,
		rpcTimeout:       rpcTimeout,
		logger:           logger,
	}
}

// GetLeader gets the leader of the cluster.
// GetLeaderResp.Leader == nil if no leader found.
func (m *Member) GetLeader(ctx context.Context) (*GetLeaderResp, error) {
	ctx, cancel := context.WithTimeout(ctx, m.rpcTimeout)
	defer cancel()
	resp, err := m.etcdCli.Get(ctx, m.leaderKey)
	if err != nil {
		return nil, ErrGetLeader.WithCause(err)
	}
	if len(resp.Kvs) > 1 {
		return nil, ErrMultipleLeader
	}
	if len(resp.Kvs) == 0 {
		return &GetLeaderResp{}, nil
	}
	leaderKv := resp.Kvs[0]
	leader := &metastoragepb.Member{}
	err = proto.Unmarshal(leaderKv.Value, leader)
	if err != nil {
		return nil, ErrInvalidLeaderValue.WithCause(err)
	}
	return &GetLeaderResp{Leader: leader, Revision: leaderKv.ModRevision}, nil
}

func (m *Member) ResetLeader(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, m.rpcTimeout)
	defer cancel()
	if _, err := m.etcdCli.Delete(ctx, m.leaderKey); err != nil {
		return ErrResetLeader.WithCause(err)
	}
	return nil
}

func (m *Member) WaitForLeaderChange(ctx context.Context, revision int64) {
	watcher := clientv3.NewWatcher(m.etcdCli)
	defer func() {
		if err := watcher.Close(); err != nil {
			m.logger.Error("close watcher failed", zap.Error(err))
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		wch := watcher.Watch(ctx, m.leaderKey, clientv3.WithRev(revision))
		for resp := range wch {
			// meet compacted error, use the compact revision.
			if resp.CompactRevision != 0 {
				m.logger.Warn("required revision has been compacted, use the compact revision",
					zap.Int64("required-revision", revision),
					zap.Int64("compact-revision", resp.CompactRevision))
				revision = resp.CompactRevision
				break
			}

			if resp.Canceled {
				m.logger.Error("watcher is cancelled", zap.Int64("revision", revision), zap.String("leader-key", m.leaderKey))
				return
			}

			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE {
					m.logger.Info("current leader is deleted", zap.String("leader-key", m.leaderKey))
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (m *Member) CampaignAndKeepLeader(ctx context.Context, leaseTTLSec int64) error {
	leaderVal, err := m.Marshal()
	if err != nil {
		return err
	}

	rawLease := clientv3.NewLease(m.etcdCli)
	newLease := newLease(rawLease, leaseTTLSec)
	closeLeaseOnce := sync.Once{}
	closeLeaseWg := sync.WaitGroup{}
	closeLease := func() {
		closeLeaseWg.Wait()
		ctx1, cancel := context.WithTimeout(context.Background(), m.rpcTimeout)
		defer cancel()
		if err := newLease.Close(ctx1); err != nil {
			m.logger.Error("close lease failed", zap.Error(err))
		}
	}
	defer closeLeaseOnce.Do(closeLease)

	ctx1, cancel := context.WithTimeout(ctx, m.rpcTimeout)
	defer cancel()
	if err := newLease.Grant(ctx1); err != nil {
		return err
	}

	// The leader key must not exist, so the CreateRevision is 0.
	cmp := clientv3.Compare(clientv3.CreateRevision(m.leaderKey), "=", 0)
	ctx1, cancel = context.WithTimeout(ctx, m.rpcTimeout)
	defer cancel()
	resp, err := m.etcdCli.
		Txn(ctx1).
		If(cmp).
		Then(clientv3.OpPut(m.leaderKey, leaderVal, clientv3.WithLease(newLease.ID))).
		Commit()
	if err != nil {
		return ErrTxnPutLeader.WithCause(err)
	} else if !resp.Succeeded {
		return ErrTxnPutLeader.WithCausef("txn put leader failed, resp:%v", resp)
	}

	m.logger.Info("succeed to set leader", zap.String("leader-key", m.leaderKey), zap.String("leader", m.Name))

	// keep the leadership after success in campaigning leader.
	closeLeaseWg.Add(1)
	go func() {
		newLease.KeepAlive(ctx)
		closeLeaseWg.Done()
		closeLeaseOnce.Do(closeLease)
	}()

	// check the leadership periodically and exit if it changes.
	leaderCheckTicker := time.NewTicker(leaderCheckInterval)
	defer leaderCheckTicker.Stop()

	for {
		select {
		case <-leaderCheckTicker.C:
			if newLease.IsExpired() {
				m.logger.Info("no longer a leader because lease has expired")
				return nil
			}
			etcdLeader := m.etcdLeaderGetter.EtcdLeaderID()
			if etcdLeader != m.ID {
				m.logger.Info("etcd leader changed and should re-assign the leadership", zap.String("old-leader", m.Name))
				return nil
			}
		case <-ctx.Done():
			m.logger.Info("server is closed")
			return nil
		}
	}
}

func (m *Member) Marshal() (string, error) {
	memPb := &metastoragepb.Member{
		Name: m.Name,
		Id:   m.ID,
	}
	bs, err := proto.Marshal(memPb)
	if err != nil {
		return "", ErrMarshalMember.WithCause(err)
	}

	return string(bs), nil
}

type GetLeaderResp struct {
	Leader   *metastoragepb.Member
	Revision int64
}
