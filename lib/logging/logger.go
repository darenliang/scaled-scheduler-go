package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.SugaredLogger

func InitLogger(level zapcore.LevelEnabler) {
	logger, err := zap.NewDevelopment(zap.IncreaseLevel(level))
	if err != nil {
		panic(err)
	}
	Logger = logger.Sugar()
}

func CheckError(err error) {
	if err != nil {
		Logger.Error(err)
	}
}
