// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
