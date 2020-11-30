package logger

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
)

type Logger int

const loggerID = "logger_id"

func (l Logger) Printf(s string, args ...interface{}) {
	log.Printf("[id=%d] %s", l, fmt.Sprintf(s, args...))
}

func (l Logger) Println(s string) {
	log.Printf("[id=%d] %s", l, s)
}

func (l Logger) Infof(s string, args ...interface{}) {
	x := fmt.Sprintf(s, args...)
	log.Infof("[id=%d] %s", l, x)
}

func (l Logger) Info(s string) {
	log.Infof("[id=%d] %s", l, s)
}

func (l Logger) Errorf(s string, args ...interface{}) {
	log.Errorf("[id=%d] %s", l, fmt.Sprintf(s, args...))
}

func (l Logger) Error(s string) {
	log.Errorf("[id=%d] %s", l, s)
}

func CtxWithLoggerID(ctx context.Context, id int) context.Context {
	return context.WithValue(ctx, loggerID, id)
}

func GetLogger(ctx context.Context) Logger {
	return Logger(ctx.Value(loggerID).(int))
}
