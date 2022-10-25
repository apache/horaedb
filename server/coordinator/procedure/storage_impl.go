// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	Version              = "v1"
	PathProcedure        = "procedure"
	PathDeletedProcedure = "deletedProcedure"
)

type EtcdStorageImpl struct {
	client    *clientv3.Client
	clusterID uint32
	rootPath  string
}

func NewEtcdStorageImpl(client *clientv3.Client, rootPath string) Storage {
	return &EtcdStorageImpl{
		client:   client,
		rootPath: rootPath,
	}
}

// CreateOrUpdate example:
// /{rootPath}/v1/procedure/{procedureID} -> {procedureType} + {procedureState} + {data}
func (e EtcdStorageImpl) CreateOrUpdate(ctx context.Context, meta *Meta) error {
	s, err := encode(meta)
	if err != nil {
		return errors.WithMessage(err, "encode meta failed")
	}
	keyPath := e.generaNormalKeyPath(meta.ID)
	opPut := clientv3.OpPut(keyPath, s)

	if _, err = e.client.Do(ctx, opPut); err != nil {
		return errors.WithMessage(err, "etcd put data failed")
	}
	return nil
}

// MarkDeleted Do a soft deletion, and the deleted key's format is:
// /{rootPath}/v1/historyProcedure/{clusterID}/{procedureID}
func (e EtcdStorageImpl) MarkDeleted(ctx context.Context, id uint64) error {
	keyPath := e.generaNormalKeyPath(id)
	meta, err := etcdutil.Get(ctx, e.client, keyPath)
	if err != nil {
		return errors.WithMessage(err, "get meta failed")
	}

	deletedKeyPath := e.generaDeletedKeyPath(id)
	opDelete := clientv3.OpDelete(keyPath)
	opPut := clientv3.OpPut(deletedKeyPath, meta)

	_, err = e.client.Txn(ctx).Then(opDelete, opPut).Commit()

	return err
}

func (e EtcdStorageImpl) List(ctx context.Context, batchSize int) ([]*Meta, error) {
	metas := make([]*Meta, 0)
	do := func(key string, value []byte) error {
		meta := &Meta{}
		if err := decode(meta, string(value)); err != nil {
			return errors.WithMessagef(err, "decode meta failed, key:%s, value:%v", key, value)
		}

		metas = append(metas, meta)
		return nil
	}

	startKey := e.generaNormalKeyPath(uint64(0))
	endKey := e.generaNormalKeyPath(math.MaxUint64)

	err := etcdutil.Scan(ctx, e.client, startKey, endKey, batchSize, do)
	if err != nil {
		return nil, errors.WithMessage(err, "scan procedure failed")
	}
	return metas, nil
}

func (e EtcdStorageImpl) generaNormalKeyPath(procedureID uint64) string {
	return e.generateKeyPath(procedureID, false)
}

func (e EtcdStorageImpl) generaDeletedKeyPath(procedureID uint64) string {
	return e.generateKeyPath(procedureID, true)
}

func (e EtcdStorageImpl) generateKeyPath(procedureID uint64, isDeleted bool) string {
	var procedurePath string
	if isDeleted {
		procedurePath = PathDeletedProcedure
	} else {
		procedurePath = PathProcedure
	}
	return path.Join(e.rootPath, Version, procedurePath, fmtID(uint64(e.clusterID)), fmtID(procedureID))
}

func fmtID(id uint64) string {
	return fmt.Sprintf("%020d", id)
}

// TODO: Use proto.Marshal replace json.Marshal
func encode(meta *Meta) (string, error) {
	bytes, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// TODO: Use proto.Unmarshal replace json.unmarshal
func decode(m *Meta, meta string) error {
	err := json.Unmarshal([]byte(meta), &m)
	return err
}
