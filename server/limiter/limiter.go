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
	"sync"

	"github.com/CeresDB/ceresmeta/server/config"
	"golang.org/x/time/rate"
)

type FlowLimiter struct {
	l *rate.Limiter
	// RWMutex is used to protect following fields.
	lock sync.RWMutex
	// limit is the updated rate of tokens.
	limit int
	// burst is the maximum number of tokens.
	burst int
	// enable is used to control the switch of the limiter.
	enable bool
}

func NewFlowLimiter(config config.LimiterConfig) *FlowLimiter {
	newLimiter := rate.NewLimiter(rate.Limit(config.Limit), config.Burst)

	return &FlowLimiter{
		l:      newLimiter,
		lock:   sync.RWMutex{},
		limit:  config.Limit,
		burst:  config.Burst,
		enable: config.Enable,
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

	f.l.SetLimit(rate.Limit(config.Limit))
	f.l.SetBurst(config.Burst)
	f.limit = config.Limit
	f.burst = config.Burst
	f.enable = config.Enable
	return nil
}

func (f *FlowLimiter) GetConfig() *config.LimiterConfig {
	return &config.LimiterConfig{
		Limit:  f.limit,
		Burst:  f.burst,
		Enable: f.enable,
	}
}
