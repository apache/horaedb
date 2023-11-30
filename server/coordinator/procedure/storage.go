/*
 * Copyright 2022 The HoraeDB Authors
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

package procedure

import (
	"context"
)

type Write interface {
	CreateOrUpdate(ctx context.Context, meta Meta) error
	CreateOrUpdateWithTTL(ctx context.Context, meta Meta, ttlSec int64) error
}

type Meta struct {
	ID      uint64
	Kind    Kind
	State   State
	RawData []byte
}

type Storage interface {
	Write
	List(ctx context.Context, procedureType Kind, batchSize int) ([]*Meta, error)
	Delete(ctx context.Context, procedureType Kind, id uint64) error
	MarkDeleted(ctx context.Context, procedureType Kind, id uint64) error
}
