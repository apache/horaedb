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

package procedure

import (
	"context"
	"testing"
	"time"

	"github.com/apache/incubator-horaedb-meta/server/etcdutil"
	"github.com/stretchr/testify/require"
)

const (
	TestClusterID       = 1
	DefaultTimeout      = time.Second * 10
	DefaultScanBatchSie = 100
	TestRootPath        = "/rootPath"
)

func testWrite(t *testing.T, storage Storage) {
	re := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	testMeta1 := Meta{
		ID:      uint64(1),
		Kind:    TransferLeader,
		State:   StateInit,
		RawData: []byte("test"),
	}

	// Test create new procedure
	err := storage.CreateOrUpdate(ctx, testMeta1)
	re.NoError(err)

	testMeta2 := Meta{
		ID:      uint64(2),
		Kind:    TransferLeader,
		State:   StateInit,
		RawData: []byte("test"),
	}
	err = storage.CreateOrUpdate(ctx, testMeta2)
	re.NoError(err)

	// Test update procedure
	testMeta2.RawData = []byte("test update")
	err = storage.CreateOrUpdate(ctx, testMeta2)
	re.NoError(err)
}

func testScan(t *testing.T, storage Storage) {
	re := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	metas, err := storage.List(ctx, TransferLeader, DefaultScanBatchSie)
	re.NoError(err)
	re.Equal(2, len(metas))
	re.Equal("test", string(metas[0].RawData))
	re.Equal("test update", string(metas[1].RawData))
}

func testDelete(t *testing.T, storage Storage) {
	re := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	testMeta1 := &Meta{
		ID:      uint64(1),
		Kind:    TransferLeader,
		State:   StateInit,
		RawData: []byte("test"),
	}
	err := storage.MarkDeleted(ctx, TransferLeader, testMeta1.ID)
	re.NoError(err)

	metas, err := storage.List(ctx, TransferLeader, DefaultScanBatchSie)
	re.NoError(err)
	re.Equal(1, len(metas))

	testMeta2 := Meta{
		ID:      uint64(2),
		Kind:    TransferLeader,
		State:   StateInit,
		RawData: []byte("test"),
	}
	err = storage.Delete(ctx, TransferLeader, testMeta2.ID)
	re.NoError(err)

	metas, err = storage.List(ctx, TransferLeader, DefaultScanBatchSie)
	re.NoError(err)
	re.Equal(0, len(metas))
}

func NewTestStorage(t *testing.T) Storage {
	_, client, _ := etcdutil.PrepareEtcdServerAndClient(t)
	storage := NewEtcdStorageImpl(client, TestRootPath, TestClusterID)
	return storage
}

func TestStorage(t *testing.T) {
	storage := NewTestStorage(t)
	testWrite(t, storage)
	testScan(t, storage)
	testDelete(t, storage)
}
