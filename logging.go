package main

import (
	"algo-runner-go/swagger"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path"
	"strings"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type logMessage swagger.LogEntryModel

func (lm *logMessage) log() {

	zapLog, err := newLogger(lm.Type_)
	if err != nil {
		panic(fmt.Sprintf("Failed to create logger! [%v]", err))
	}
	log = zapr.NewLogger(zapLog)

	// Send to local console and file
	if lm.Status == "Unknown" ||
		lm.Status == "Failed" ||
		lm.Status == "Terminated" ||
		lm.Status == "Timeout" {
		log.Error(errors.New(lm.Msg),
			"version", lm.Version,
			"status", lm.Status,
			"runId", lm.RunId,
			"isError", true,
			"data", lm.Data)
	} else {
		log.Info(lm.Msg,
			"version", lm.Version,
			"status", lm.Status,
			"runId", lm.RunId,
			"isError", false,
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
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	cfg.OutputPaths = []string{
		"stdout",
		fullPathFile,
	}
	return cfg.Build()
}
