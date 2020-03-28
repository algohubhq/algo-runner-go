package main

import (
	"algo-runner-go/openapi"
	"fmt"
	"os"
	"os/user"
	"path"
	"strings"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
)

type logMessage openapi.LogEntryModel

func (lm *logMessage) log(errLog error) {

	zapLog, err := newLogger(string(lm.Type))
	if err != nil {
		panic(fmt.Sprintf("Failed to create logger! [%v]", err))
	}
	log = zapr.NewLogger(zapLog)

	// Send to local console and file
	if errLog != nil {

		// Increment the error metric
		switch logType := strings.ToLower(string(lm.Type)); logType {
		case "algo":
			algoErrorCounter.WithLabelValues(deploymentLabel, pipelineLabel, componentLabel, algoLabel, algoVersionLabel, algoIndexLabel).Inc()
		case "runner":
			runnerErrorCounter.WithLabelValues(deploymentLabel, pipelineLabel, componentLabel, algoLabel, algoVersionLabel, algoIndexLabel).Inc()
		}

		log.Error(errLog,
			lm.Msg,
			"version", lm.Version,
			"type", lm.Type,
			"traceId", lm.TraceId,
			"data", lm.Data)
	} else {
		log.Info(lm.Msg,
			"version", lm.Version,
			"type", lm.Type,
			"traceId", lm.TraceId,
			"data", lm.Data)
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
