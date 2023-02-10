// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
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
		Typ:     TransferLeader,
		State:   StateInit,
		RawData: []byte("test"),
	}

	// Test create new procedure
	err := storage.CreateOrUpdate(ctx, testMeta1)
	re.NoError(err)

	testMeta2 := Meta{
		ID:      uint64(2),
		Typ:     TransferLeader,
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

	metas, err := storage.List(ctx, DefaultScanBatchSie)
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
		Typ:     TransferLeader,
		State:   StateInit,
		RawData: []byte("test"),
	}
	err := storage.MarkDeleted(ctx, testMeta1.ID)
	re.NoError(err)

	metas, err := storage.List(ctx, DefaultScanBatchSie)
	re.NoError(err)
	re.Equal(1, len(metas))
}

func NewTestStorage(t *testing.T) Storage {
	_, client, _ := etcdutil.PrepareEtcdServerAndClient(t)
	storage := NewEtcdStorageImpl(client, TestRootPath)
	return storage
}

func TestStorage(t *testing.T) {
	storage := NewTestStorage(t)
	testWrite(t, storage)
	testScan(t, storage)
	testDelete(t, storage)
}
