package logging

import (
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	"fmt"
	"os"
	"os/user"
	"path"
	"strings"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/prometheus/common/log"
	"go.uber.org/zap"
)

type Logger struct {
	LogMessage *openapi.LogEntryModel
	Metrics    *metrics.Metrics
	log        *logr.Logger
}

// NewLogger returns a new Logger struct
func NewLogger(logMessage *openapi.LogEntryModel, metrics *metrics.Metrics) Logger {

	zapLog, err := newLogger(string(*logMessage.Type))
	if err != nil {
		panic(fmt.Sprintf("Failed to create logger! [%v]", err))
	}
	log := zapr.NewLogger(zapLog)

	return Logger{
		LogMessage: logMessage,
		Metrics:    metrics,
		log:        &log,
	}
}

func (l *Logger) Info(msg string) {

	log.Info(l.LogMessage.Msg,
		"version", l.LogMessage.Version,
		"type", l.LogMessage.Type,
		"level", openapi.LOGLEVELS_INFO,
		"traceId", l.LogMessage.TraceId,
		"data", l.LogMessage.Data)

}

func (l *Logger) Debug(msg string) {

	log.Debug(l.LogMessage.Msg,
		"version", l.LogMessage.Version,
		"type", l.LogMessage.Type,
		"level", openapi.LOGLEVELS_DEBUG,
		"traceId", l.LogMessage.TraceId,
		"data", l.LogMessage.Data)

}

func (l *Logger) Error(msg string, errLog error) {

	if errLog != nil {
		l.incrementError()
		log.Error(errLog,
			l.LogMessage.Msg,
			"version", l.LogMessage.Version,
			"type", l.LogMessage.Type,
			"level", openapi.LOGLEVELS_ERROR,
			"traceId", l.LogMessage.TraceId,
			"data", l.LogMessage.Data)
	} else {
		log.Error(l.LogMessage.Msg,
			"version", l.LogMessage.Version,
			"type", l.LogMessage.Type,
			"level", openapi.LOGLEVELS_ERROR,
			"traceId", l.LogMessage.TraceId,
			"data", l.LogMessage.Data)
	}

}

func (l *Logger) Warn(msg string, errLog error) {

	if errLog != nil {
		l.incrementError()
		log.Warn(errLog,
			l.LogMessage.Msg,
			"version", l.LogMessage.Version,
			"type", l.LogMessage.Type,
			"level", openapi.LOGLEVELS_WARNING,
			"traceId", l.LogMessage.TraceId,
			"data", l.LogMessage.Data)
	} else {
		log.Warn(l.LogMessage.Msg,
			"version", l.LogMessage.Version,
			"type", l.LogMessage.Type,
			"level", openapi.LOGLEVELS_WARNING,
			"traceId", l.LogMessage.TraceId,
			"data", l.LogMessage.Data)
	}

}

func (l *Logger) incrementError() {

	// Increment the error metric
	switch logType := strings.ToLower(string(*l.LogMessage.Type)); logType {
	case "algo":
		if l.Metrics != nil {
			l.Metrics.AlgoErrorCounter.WithLabelValues(l.Metrics.DeploymentLabel,
				l.Metrics.PipelineLabel,
				l.Metrics.ComponentLabel,
				l.Metrics.AlgoLabel,
				l.Metrics.AlgoVersionLabel,
				l.Metrics.AlgoIndexLabel).Inc()
		}
	case "runner":
		if l.Metrics != nil {
			l.Metrics.RunnerErrorCounter.WithLabelValues(l.Metrics.DeploymentLabel,
				l.Metrics.PipelineLabel,
				l.Metrics.ComponentLabel,
				l.Metrics.AlgoLabel,
				l.Metrics.AlgoVersionLabel,
				l.Metrics.AlgoIndexLabel).Inc()
		}
	}

}

func newLogger(logType string) (*zap.Logger, error) {

	usr, _ := user.Current()
	dir := usr.HomeDir
	folder := path.Join(dir, "algorun", "logs")
	if _, err := os.Stat(folder); os.IsNotExist(err) {
		os.MkdirAll(folder, os.ModePerm)
	}
	fullPathFile := path.Join(folder, fmt.Sprintf("%s.log", strings.ToLower(logType)))

	cfg := zap.NewProductionConfig()
	// cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	cfg.OutputPaths = []string{
		"stdout",
		fullPathFile,
	}
	return cfg.Build()
}
