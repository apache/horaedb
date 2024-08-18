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

	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/operation/transferleader"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/stretchr/testify/require"
)

type mockProcedure struct {
	ClusterID        storage.ClusterID
	clusterVersion   uint64
	kind             procedure.Kind
	ShardWithVersion map[storage.ShardID]uint64
}

func (m mockProcedure) ID() uint64 {
	return 0
}

func (m mockProcedure) Kind() procedure.Kind {
	return m.kind
}

func (m mockProcedure) Start(_ context.Context) error {
	return nil
}

func (m mockProcedure) Cancel(_ context.Context) error {
	return nil
}

func (m mockProcedure) State() procedure.State {
	return procedure.StateInit
}

func (m mockProcedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return procedure.RelatedVersionInfo{
		ClusterID:        m.ClusterID,
		ShardWithVersion: m.ShardWithVersion,
		ClusterVersion:   m.clusterVersion,
	}
}

func (m mockProcedure) Priority() procedure.Priority {
	return procedure.PriorityLow
}

func TestBatchProcedure(t *testing.T) {
	re := require.New(t)
	var procedures []procedure.Procedure

	// Procedures with same type and version.
	for i := 0; i < 3; i++ {
		shardWithVersion := map[storage.ShardID]uint64{}
		shardWithVersion[storage.ShardID(i)] = 0
		p := CreateMockProcedure(storage.ClusterID(0), 0, 0, shardWithVersion)
		procedures = append(procedures, p)
	}
	_, err := transferleader.NewBatchTransferLeaderProcedure(0, procedures)
	re.NoError(err)

	// Procedure with different clusterID.
	for i := 0; i < 3; i++ {
		shardWithVersion := map[storage.ShardID]uint64{}
		shardWithVersion[storage.ShardID(i)] = 0
		p := CreateMockProcedure(storage.ClusterID(i), 0, procedure.TransferLeader, shardWithVersion)
		procedures = append(procedures, p)
	}
	_, err = transferleader.NewBatchTransferLeaderProcedure(0, procedures)
	re.Error(err)

	// Procedures with different type.
	for i := 0; i < 3; i++ {
		shardWithVersion := map[storage.ShardID]uint64{}
		shardWithVersion[storage.ShardID(i)] = 0
		p := CreateMockProcedure(0, 0, procedure.Kind(i), shardWithVersion)
		procedures = append(procedures, p)
	}
	_, err = transferleader.NewBatchTransferLeaderProcedure(0, procedures)
	re.Error(err)

	// Procedures with different version.
	for i := 0; i < 3; i++ {
		shardWithVersion := map[storage.ShardID]uint64{}
		shardWithVersion[storage.ShardID(0)] = uint64(i)
		p := CreateMockProcedure(0, 0, procedure.Kind(i), shardWithVersion)
		procedures = append(procedures, p)
	}
	_, err = transferleader.NewBatchTransferLeaderProcedure(0, procedures)
	re.Error(err)
}

func CreateMockProcedure(clusterID storage.ClusterID, clusterVersion uint64, typ procedure.Kind, shardWithVersion map[storage.ShardID]uint64) procedure.Procedure {
	return mockProcedure{
		ClusterID:        clusterID,
		clusterVersion:   clusterVersion,
		kind:             typ,
		ShardWithVersion: shardWithVersion,
	}
}
