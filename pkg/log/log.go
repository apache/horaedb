// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package log

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func init() {
	defaultConfig := &Config{
		Level: "info",
		File:  "stdout",
	}
	_, err := InitGlobalLogger(defaultConfig)
	if err != nil {
		fmt.Println("fail to init global logger, err:", err)
	}
}

var (
	globalLogger    *zap.Logger
	globalLoggerCfg *zap.Config
)

// InitGlobalLogger initializes the global logger with Config.
func InitGlobalLogger(cfg *Config) (*zap.Logger, error) {
	zapCfg := DefaultZapLoggerConfig

	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}
	zapCfg.Level.SetLevel(level)

	if len(cfg.File) > 0 {
		zapCfg.OutputPaths = []string{cfg.File}
		zapCfg.ErrorOutputPaths = []string{cfg.File}
	}

	logger, err := zapCfg.Build()
	if err != nil {
		return nil, err
	}

	globalLogger = logger
	globalLoggerCfg = &zapCfg
	return logger, nil
}

func GetLogger() *zap.Logger {
	return globalLogger
}

func GetLoggerConfig() *zap.Config {
	return globalLoggerCfg
}
