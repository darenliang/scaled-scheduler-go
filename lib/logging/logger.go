package logging

import (
	"github.com/darenliang/scaled-scheduler-go/lib/interfaces"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.SugaredLogger

// InitLogger initializes the logger.
func InitLogger(level zapcore.LevelEnabler) {
	logger, err := zap.NewDevelopment(zap.IncreaseLevel(level))
	if err != nil {
		panic(err)
	}
	Logger = logger.Sugar()
}

// CheckError logs an error if it is not nil.
func CheckError(err error) {
	if err != nil {
		Logger.Error(err)
	}
}

func LogRecvProtocolMessage(source string, msgType interfaces.Stringer, payloads ...interfaces.Serializable) {
	LogProtocolMessage("recv", source, msgType, payloads...)
}

func LogSendProtocolMessage(source string, msgType interfaces.Stringer, payloads ...interfaces.Serializable) {
	LogProtocolMessage("send", source, msgType, payloads...)
}

func LogProtocolMessage(msg, source string, msgType interfaces.Stringer, payloads ...interfaces.Serializable) {
	if len(payloads) == 0 {
		Logger.Debugw(msg, "source", source, "type", msgType.String())
	} else {
		Logger.Debugw(msg, "source", source, "type", msgType.String(), zap.Object("payload", payloads[0]))
	}
}
