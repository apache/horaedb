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

package transferleader_test

import (
	"context"
	"testing"

	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/operation/transferleader"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/test"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/stretchr/testify/require"
)

func TestTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := test.MockDispatch{}
	c := test.InitEmptyCluster(ctx, t)
	s := test.NewTestStorage(t)

	snapshot := c.GetMetadata().GetClusterSnapshot()

	var targetShardID storage.ShardID
	for shardID := range snapshot.Topology.ShardViewsMapping {
		targetShardID = shardID
		break
	}
	newLeaderNodeName := snapshot.RegisteredNodes[0].Node.Name

	p, err := transferleader.NewProcedure(transferleader.ProcedureParams{
		ID:                0,
		Dispatch:          dispatch,
		Storage:           s,
		ClusterSnapshot:   snapshot,
		ShardID:           targetShardID,
		OldLeaderNodeName: "",
		NewLeaderNodeName: newLeaderNodeName,
	})
	re.NoError(err)

	err = p.Start(ctx)
	re.NoError(err)
}
