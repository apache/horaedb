/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package static_test

import (
	"context"
	"testing"

	"github.com/apache/incubator-horaedb-meta/server/coordinator"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/test"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/scheduler/nodepicker"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/scheduler/static"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestStaticTopologyScheduler(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	procedureFactory := coordinator.NewFactory(zap.NewNop(), test.MockIDAllocator{}, test.MockDispatch{}, test.NewTestStorage(t))

	s := static.NewShardScheduler(procedureFactory, nodepicker.NewConsistentUniformHashNodePicker(zap.NewNop()), 1)

	// EmptyCluster would be scheduled an empty procedure.
	emptyCluster := test.InitEmptyCluster(ctx, t)
	result, err := s.Schedule(ctx, emptyCluster.GetMetadata().GetClusterSnapshot())
	re.NoError(err)
	re.Empty(result)

	// PrepareCluster would be scheduled a transfer leader procedure.
	prepareCluster := test.InitPrepareCluster(ctx, t)
	result, err = s.Schedule(ctx, prepareCluster.GetMetadata().GetClusterSnapshot())
	re.NoError(err)
	re.NotEmpty(result)

	// StableCluster with all shards assigned would be scheduled a transfer leader procedure by hash rule.
	stableCluster := test.InitStableCluster(ctx, t)
	result, err = s.Schedule(ctx, stableCluster.GetMetadata().GetClusterSnapshot())
	re.NoError(err)
	re.NotEmpty(result)
}
