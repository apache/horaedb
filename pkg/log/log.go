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
