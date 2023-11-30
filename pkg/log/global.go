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

package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func Debug(msg string, fields ...zap.Field) {
	globalLogger.WithOptions(zap.AddCallerSkip(1)).Debug(msg, fields...)
}

func Info(msg string, fields ...zap.Field) {
	globalLogger.WithOptions(zap.AddCallerSkip(1)).Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	globalLogger.WithOptions(zap.AddCallerSkip(1)).Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	globalLogger.WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
}

func Panic(msg string, fields ...zap.Field) {
	globalLogger.WithOptions(zap.AddCallerSkip(1)).Panic(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	globalLogger.Fatal(msg, fields...)
}

func With(fields ...zap.Field) *zap.Logger {
	return globalLogger.With(fields...)
}

func SetLevel(lvl string) error {
	level, err := zapcore.ParseLevel(lvl)
	if err != nil {
		return err
	}
	globalLoggerCfg.Level.SetLevel(level)
	return nil
}

func GetLevel() zapcore.Level {
	return globalLoggerCfg.Level.Level()
}
