package main

import (
	"algo-runner-go/swagger"
	"fmt"
	"os"
	"os/user"
	"path"
	"strings"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
)

type logMessage swagger.LogEntryModel

func (lm *logMessage) log(errLog error) {

	zapLog, err := newLogger(lm.Type_)
	if err != nil {
		panic(fmt.Sprintf("Failed to create logger! [%v]", err))
	}
	log = zapr.NewLogger(zapLog)

	// Send to local console and file
	if errLog != nil {

		// Increment the error metric
		switch logType := strings.ToLower(lm.Type_); logType {
		case "algo":
			algoErrorCounter.WithLabelValues(deploymentLabel, algoLabel, algoVersionLabel, algoIndexLabel).Inc()
		case "runner":
			runnerErrorCounter.WithLabelValues(deploymentLabel, algoLabel, algoVersionLabel, algoIndexLabel).Inc()
		}

		log.Error(errLog,
			lm.Msg,
			"version", lm.Version,
			"type", lm.Type_,
			"status", lm.Status,
			"runId", lm.RunId,
			"data", lm.Data)
	} else {
		log.Info(lm.Msg,
			"version", lm.Version,
			"type", lm.Type_,
			"status", lm.Status,
			"runId", lm.RunId,
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
