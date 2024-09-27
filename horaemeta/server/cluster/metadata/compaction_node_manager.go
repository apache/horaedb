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

package metadata

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// CompactionNodeManager manages the compaction nodes, including monitor and schedule the registered nodes.
type CompactionNodeManager interface {
	RegisterCompactionNode(registeredNode RegisteredNode)
	GetRegisteredCompactionNodes() []RegisteredNode
	GetRegisteredCompactionNodeByName(nodeName string) (RegisteredNode, bool)
	GetCompactionNodesKeyList() []string

	FetchCompactionNode() (string, error)
}

type CompactionNodeManagerImpl struct {
	logger *zap.Logger

	// RWMutex is used to protect following field.
	lock sync.RWMutex
	// Manage the registered compaction nodes from heartbeat.
	registeredCompactionNodesCache map[string]RegisteredNode
	// Maintain a list to store the keys (node name here) of compaction nodes.
	compactionNodesKeyList []string
	// The index of key stored in compactionNodesKeyList which is accessed most recently.
	index int
}

func NewCompactionNodeManagerImpl(logger *zap.Logger) CompactionNodeManager {
	return &CompactionNodeManagerImpl{
		logger:                         logger,
		lock:                           sync.RWMutex{},
		registeredCompactionNodesCache: map[string]RegisteredNode{},
		compactionNodesKeyList:         []string{},
		index:                          -1,
	}
}

func (m *CompactionNodeManagerImpl) RegisterCompactionNode(registeredNode RegisteredNode) {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, exists := m.registeredCompactionNodesCache[registeredNode.Node.Name]
	m.registeredCompactionNodesCache[registeredNode.Node.Name] = registeredNode

	if !exists {
		m.compactionNodesKeyList = append(m.compactionNodesKeyList, registeredNode.Node.Name)
	}
}

// FetchCompactionNode selects registered compaction node with round-robin strategy and return its name.
func (m *CompactionNodeManagerImpl) FetchCompactionNode() (string, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for {
		if len(m.compactionNodesKeyList) == 0 {
			m.index = -1
			return "", ErrFetchCompactionNode
		}

		// Get next compaction node.
		m.index = (m.index + 1) % len(m.compactionNodesKeyList)
		nodeName := m.compactionNodesKeyList[m.index]
		node := m.registeredCompactionNodesCache[nodeName]

		// Check if the node is valid.
		now := time.Now()
		if node.IsExpired(now) {
			m.RemoveNode(m.index)
			continue
		}

		return nodeName, nil
	}
}

// RemoveNode deletes the node (whose index is idx) from map and key list.
func (m *CompactionNodeManagerImpl) RemoveNode(idx int) {
	// RemoveNode will only be called in FetchCompactionNode, so it's no need to lock.

	nodeName := m.compactionNodesKeyList[idx]
	m.compactionNodesKeyList = append(m.compactionNodesKeyList[:idx], m.compactionNodesKeyList[idx+1:]...)
	delete(m.registeredCompactionNodesCache, nodeName)

	// Adjust the index.
	m.index--
}

func (m *CompactionNodeManagerImpl) GetRegisteredCompactionNodes() []RegisteredNode {
	m.lock.RLock()
	defer m.lock.RUnlock()

	nodes := make([]RegisteredNode, 0, len(m.registeredCompactionNodesCache))
	for _, node := range m.registeredCompactionNodesCache {
		nodes = append(nodes, node)
	}
	return nodes
}

func (m *CompactionNodeManagerImpl) GetRegisteredCompactionNodeByName(nodeName string) (RegisteredNode, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	registeredNode, ok := m.registeredCompactionNodesCache[nodeName]
	return registeredNode, ok
}

func (m *CompactionNodeManagerImpl) GetCompactionNodesKeyList() []string {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.compactionNodesKeyList
}
