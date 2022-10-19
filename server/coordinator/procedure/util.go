// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Cancel event when error is not nil. If error is nil, do nothing.
func cancelEventWithLog(event *fsm.Event, err error, msg string, fields ...zap.Field) {
	if err == nil {
		return
	}
	fields = append(fields, zap.Error(err))
	log.Error(msg, fields...)
	event.Cancel(errors.WithMessage(err, msg))
}
