// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package limiter

import (
	"sync"

	"github.com/CeresDB/ceresmeta/server/config"
	"golang.org/x/time/rate"
)

type FlowLimiter struct {
	l *rate.Limiter
	// RWMutex is used to protect following fields.
	lock                          sync.RWMutex
	tokenBucketFillRate           int
	tokenBucketBurstEventCapacity int
	enable                        bool
}

func NewFlowLimiter(config config.LimiterConfig) *FlowLimiter {
	newLimiter := rate.NewLimiter(rate.Limit(config.TokenBucketFillRate), config.TokenBucketBurstEventCapacity)

	return &FlowLimiter{
		l:                             newLimiter,
		tokenBucketFillRate:           config.TokenBucketFillRate,
		tokenBucketBurstEventCapacity: config.TokenBucketBurstEventCapacity,
		enable:                        config.Enable,
	}
}

func (f *FlowLimiter) Allow() bool {
	if !f.enable {
		return true
	}
	return f.l.Allow()
}

func (f *FlowLimiter) UpdateLimiter(config config.LimiterConfig) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.l.SetLimit(rate.Limit(config.TokenBucketFillRate))
	f.l.SetBurst(config.TokenBucketBurstEventCapacity)
	f.tokenBucketFillRate = config.TokenBucketFillRate
	f.tokenBucketBurstEventCapacity = config.TokenBucketBurstEventCapacity
	f.enable = config.Enable
	return nil
}
