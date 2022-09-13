// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Storage is the interface for the backend storage of the ceresmeta.
type Storage interface {
	MetaStorage
}

// NewStorageWithEtcdBackend creates a new storage with etcd backend.
func NewStorageWithEtcdBackend(client *clientv3.Client, rootPath string, opts Options) Storage {
	return newEtcdStorage(client, rootPath, opts)
}
