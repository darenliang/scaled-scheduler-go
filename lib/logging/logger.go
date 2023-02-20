package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.SugaredLogger

func InitLogger(level zapcore.Level) {
	logger, err := zap.NewDevelopment(zap.IncreaseLevel(level))
	if err != nil {
		panic(err)
	}
	Logger = logger.Sugar()
}
