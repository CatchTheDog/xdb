package xdb

import "go.uber.org/zap"

var slogger *zap.SugaredLogger // 日志对象

func init() {
	logger, _ := zap.NewProduction()
	slogger = logger.Sugar()
}
