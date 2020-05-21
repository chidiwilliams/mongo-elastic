package logger

import "go.uber.org/zap"

// Log is the default app logger
var Log *zap.SugaredLogger

func init() {
	logger, _ := zap.NewDevelopment()
	Log = logger.Sugar()
}
