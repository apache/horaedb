/*
 * Copyright 2022 The HoraeDB Authors
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

package member

import (
	"context"
	"sync"
	"time"

	"github.com/CeresDB/horaemeta/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// lease helps use etcd lease by providing Grant, Close and auto renewing the lease.
type lease struct {
	rawLease clientv3.Lease
	// timeout is the rpc timeout and always equals to the ttlSec.
	timeout time.Duration
	ttlSec  int64
	// logger will be updated after Grant is called.
	logger *zap.Logger

	// The fields below are initialized after Grant is called.
	ID clientv3.LeaseID

	expireTimeL sync.RWMutex
	// expireTime helps determine the lease whether is expired.
	expireTime time.Time
}

func newLease(rawLease clientv3.Lease, ttlSec int64) *lease {
	return &lease{
		rawLease: rawLease,
		timeout:  time.Duration(ttlSec) * time.Second,
		ttlSec:   ttlSec,
		logger:   log.GetLogger(),

		ID:          0,
		expireTimeL: sync.RWMutex{},
		expireTime:  time.Time{},
	}
}

type renewLeaseResult int

const (
	renewLeaseAlive renewLeaseResult = iota
	renewLeaseFailed
	renewLeaseExpired
)

func (r renewLeaseResult) failed() bool {
	return r == renewLeaseFailed
}

func (r renewLeaseResult) alive() bool {
	return r == renewLeaseAlive
}

func (l *lease) Grant(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	leaseResp, err := l.rawLease.Grant(ctx, l.ttlSec)
	if err != nil {
		return ErrGrantLease.WithCause(err)
	}

	l.ID = leaseResp.ID
	l.logger = log.With(zap.Int64("lease-id", int64(leaseResp.ID)))

	expiredAt := time.Now().Add(time.Second * time.Duration(leaseResp.TTL))
	l.setExpireTime(expiredAt)

	l.logger.Debug("lease is granted", zap.Time("expired-at", expiredAt))
	return nil
}

func (l *lease) Close(ctx context.Context) error {
	// Check whether the lease was granted.
	if l.ID == 0 {
		return nil
	}

	// Release and reset all the resources.
	l.setExpireTime(time.Time{})
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	if _, err := l.rawLease.Revoke(ctx, l.ID); err != nil {
		return ErrRevokeLease.WithCause(err)
	}
	if err := l.rawLease.Close(); err != nil {
		return ErrCloseLease.WithCause(err)
	}

	return nil
}

// KeepAlive renews the lease until timeout for renewing lease.
func (l *lease) KeepAlive(ctx context.Context) {
	// Used to receive the renewed event.
	renewed := make(chan bool, 1)
	ctx1, cancelRenewBg := context.WithCancel(ctx)
	// Used to join with the renew goroutine in the background.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		l.renewLeaseBg(ctx1, l.timeout/3, renewed)
		wg.Done()
	}()

L:
	for {
		select {
		case alive := <-renewed:
			l.logger.Debug("received renew result", zap.Bool("renew-alive", alive))
			if !alive {
				break L
			}
		case <-time.After(l.timeout):
			l.logger.Warn("lease timeout, stop keeping lease alive")
			break L
		case <-ctx.Done():
			l.logger.Info("stop keeping lease alive because ctx is done")
			break L
		}
	}

	cancelRenewBg()
	wg.Wait()
}

// IsExpired is goroutine safe.
func (l *lease) IsExpired() bool {
	expiredAt := l.getExpireTime()
	return time.Now().After(expiredAt)
}

func (l *lease) setExpireTime(newExpireTime time.Time) {
	l.expireTimeL.Lock()
	defer l.expireTimeL.Unlock()

	l.expireTime = newExpireTime
}

// `setExpireTimeIfNewer` updates the l.expireTime only if the newExpireTime is after l.expireTime.
// Returns true if l.expireTime is updated.
func (l *lease) setExpireTimeIfNewer(newExpireTime time.Time) bool {
	l.expireTimeL.Lock()
	defer l.expireTimeL.Unlock()

	if newExpireTime.After(l.expireTime) {
		l.expireTime = newExpireTime
		return true
	}

	return false
}

func (l *lease) getExpireTime() time.Time {
	l.expireTimeL.RLock()
	defer l.expireTimeL.RUnlock()

	return l.expireTime
}

// `renewLeaseBg` keeps the lease alive by periodically call `lease.KeepAliveOnce`.
// The l.expireTime will be updated during renewing and the renew lease result (whether alive) will be told to caller by `renewed` channel.
func (l *lease) renewLeaseBg(ctx context.Context, interval time.Duration, renewed chan<- bool) {
	l.logger.Info("start renewing lease background", zap.Duration("interval", interval))
	defer l.logger.Info("stop renewing lease background", zap.Duration("interval", interval))

L:
	for {
		renewOnce := func() renewLeaseResult {
			start := time.Now()
			ctx1, cancel := context.WithTimeout(ctx, l.timeout)
			defer cancel()
			resp, err := l.rawLease.KeepAliveOnce(ctx1, l.ID)
			if err != nil {
				l.logger.Error("lease keep alive failed", zap.Error(err))
				return renewLeaseFailed
			}
			if resp.TTL < 0 {
				l.logger.Warn("lease is expired")
				return renewLeaseExpired
			}

			expireAt := start.Add(time.Duration(resp.TTL) * time.Second)
			updated := l.setExpireTimeIfNewer(expireAt)
			l.logger.Debug("got next expired time", zap.Time("expired-at", expireAt), zap.Bool("updated", updated))
			return renewLeaseAlive
		}

		renewRes := renewOnce()

		// Init the timer for next keep alive action.
		t := time.After(interval)

		if !renewRes.failed() {
			// Notify result of the renew.
			select {
			case renewed <- renewRes.alive():
			case <-ctx.Done():
				break L
			}
		}

		// Wait for next keep alive action.
		select {
		case <-t:
		case <-ctx.Done():
			break L
		}
	}
}
