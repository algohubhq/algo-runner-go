package main

import (
	"algo-runner-go/swagger"
	"fmt"
	"os"
	"os/user"
	"path"
	"strings"
	"time"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
)

type logMessage swagger.LogMessage

func (lm *logMessage) log() {

	zapLog, err := newLogger(lm.LogMessageType)
	if err != nil {
		panic(fmt.Sprintf("Failed to create logger! [%v]", err))
	}
	log = zapr.NewLogger(zapLog)

	lm.LogTimestamp = time.Now().UTC()

	// Send to local console and file
	log.Info("", "data", lm)

}

func newLogger(logMessageType string) (*zap.Logger, error) {

	usr, _ := user.Current()
	dir := usr.HomeDir
	folder := path.Join(dir, "algorun", "logs")
	if _, err := os.Stat(folder); os.IsNotExist(err) {
		os.MkdirAll(folder, os.ModePerm)
	}
	fullPathFile := path.Join(folder, fmt.Sprintf("%s.log", strings.ToLower(logMessageType)))

	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{
		fullPathFile,
	}
	return cfg.Build()
}
