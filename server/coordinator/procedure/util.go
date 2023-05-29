// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
