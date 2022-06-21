// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	DefaultLogLevel = "info"
	DefaultLogFile  = "/tmp/ceresmeta/out.log"
)

type Config struct {
	Level string `toml:"level" json:"level"`
	File  string `toml:"file" json:"file"`
}

// DefaultZapLoggerConfig defines default zap logger configuration.
var DefaultZapLoggerConfig = zap.Config{
	Level:       zap.NewAtomicLevelAt(zapcore.InfoLevel),
	Development: false,
	Sampling: &zap.SamplingConfig{
		Initial:    100,
		Thereafter: 100,
	},
	Encoding: "console",
	EncoderConfig: zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	},
	OutputPaths:      []string{"stdout"},
	ErrorOutputPaths: []string{"stdout"},
}
