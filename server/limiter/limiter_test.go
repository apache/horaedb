/*
 * Copyright 2022 The CeresDB Authors
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

package limiter

import (
	"testing"
	"time"

	"github.com/CeresDB/horaemeta/server/config"
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
