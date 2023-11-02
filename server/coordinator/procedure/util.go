/*
 * Copyright 2022 The CeresDB Authors
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
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// CancelEventWithLog Cancel event when error is not nil. If error is nil, do nothing.
func CancelEventWithLog(event *fsm.Event, err error, msg string, fields ...zap.Field) {
	if err == nil {
		return
	}
	fields = append(fields, zap.Error(err))
	log.Error(msg, fields...)
	event.Cancel(errors.WithMessage(err, msg))
}

// nolint
func GetRequestFromEvent[T any](event *fsm.Event) (T, error) {
	if len(event.Args) != 1 {
		return *new(T), ErrGetRequest.WithCausef("event args length must be 1, actual length:%v", len(event.Args))
	}

	switch request := event.Args[0].(type) {
	case T:
		return request, nil
	default:
		return *new(T), ErrGetRequest.WithCausef("event arg type must be same as return type")
	}
}
