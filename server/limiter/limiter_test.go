// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package limiter

import (
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/config"
	"github.com/stretchr/testify/require"
)

const (
	defaultInitialLimiterRate     = 10 * 1000
	defaultInitialLimiterCapacity = 1000
	defaultEnableLimiter          = true
	defaultUpdateLimiterRate      = 100 * 1000
	defaultUpdateLimiterCapacity  = 100 * 1000
)

func TestFlowLimiter(t *testing.T) {
	re := require.New(t)
	flowLimiter := NewFlowLimiter(config.LimiterConfig{
		Limit:  defaultInitialLimiterRate,
		Burst:  defaultInitialLimiterCapacity,
		Enable: defaultEnableLimiter,
	})

	for i := 0; i < defaultInitialLimiterCapacity; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}

	time.Sleep(time.Millisecond)
	for i := 0; i < defaultInitialLimiterRate/1000; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}

	err := flowLimiter.UpdateLimiter(config.LimiterConfig{
		Limit:  defaultUpdateLimiterRate,
		Burst:  defaultUpdateLimiterCapacity,
		Enable: defaultEnableLimiter,
	})
	re.NoError(err)

	limiter := flowLimiter.GetConfig()
	re.Equal(defaultUpdateLimiterRate, limiter.Limit)
	re.Equal(defaultUpdateLimiterCapacity, limiter.Burst)
	re.Equal(defaultEnableLimiter, limiter.Enable)

	time.Sleep(time.Millisecond)
	for i := 0; i < defaultUpdateLimiterRate/1000; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}
}
