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

package metadata_test

import (
	"context"
	"testing"
	"time"

	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	TestCompactionNodeName  = "TestCompactionNodeName"
	TestCompactionNodeName1 = "TestCompactionNodeName1"
)

func TestCompactionManager(t *testing.T) {
	ctx := context.Background()
	re := require.New(t)

	compactionNodeManager := metadata.NewCompactionNodeManagerImpl(zap.NewNop())

	testRegisterCompactionNode(ctx, re, compactionNodeManager)
	testFetchCompactionNode(ctx, re, compactionNodeManager)
}

func testRegisterCompactionNode(_ context.Context, re *require.Assertions, manager metadata.CompactionNodeManager) {
	// Register a compaction node.
	lastTouchTime := uint64(time.Now().UnixMilli())
	manager.RegisterCompactionNode(metadata.RegisteredNode{
		Node: storage.Node{
			Name:          TestCompactionNodeName,
			NodeStats:     storage.NewCompactionNodeStats(),
			LastTouchTime: lastTouchTime,
			State:         0,
		},
		ShardInfos: nil,
	})

	keyList := manager.GetCompactionNodesKeyList()
	re.Equal(1, len(keyList))
	re.Equal(TestCompactionNodeName, keyList[0])
	re.Equal(1, len(manager.GetRegisteredCompactionNodes()))

	node, exists := manager.GetRegisteredCompactionNodeByName(TestCompactionNodeName)
	re.Equal(exists, true)
	re.Equal(lastTouchTime, node.Node.LastTouchTime)

	// Update lastTouchTime
	lastTouchTime = uint64(time.Now().UnixMilli())
	node.Node.LastTouchTime = lastTouchTime
	manager.RegisterCompactionNode(node)

	keyList = manager.GetCompactionNodesKeyList()
	re.Equal(1, len(keyList))
	re.Equal(TestCompactionNodeName, keyList[0])
	re.Equal(1, len(manager.GetRegisteredCompactionNodes()))

	node, exists = manager.GetRegisteredCompactionNodeByName(TestCompactionNodeName)
	re.Equal(exists, true)
	re.Equal(lastTouchTime, node.Node.LastTouchTime)

	// Register another compaction node.
	lastTouchTime = uint64(time.Now().UnixMilli())
	manager.RegisterCompactionNode(metadata.RegisteredNode{
		Node: storage.Node{
			Name:          TestCompactionNodeName1,
			NodeStats:     storage.NewCompactionNodeStats(),
			LastTouchTime: lastTouchTime,
			State:         0,
		},
		ShardInfos: nil,
	})

	keyList = manager.GetCompactionNodesKeyList()
	re.Equal(2, len(keyList))
	re.Equal(TestCompactionNodeName1, keyList[1])
	re.Equal(2, len(manager.GetRegisteredCompactionNodes()))
}

func testFetchCompactionNode(_ context.Context, re *require.Assertions, manager metadata.CompactionNodeManager) {
	nodeName, err := manager.FetchCompactionNode()
	re.NoError(err)
	re.Equal(nodeName, TestCompactionNodeName)

	nodeName, err = manager.FetchCompactionNode()
	re.NoError(err)
	re.Equal(nodeName, TestCompactionNodeName1)
}
