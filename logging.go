package main

import (
	"algo-runner-go/swagger"
	"fmt"
	"log"
	"os"
	"os/user"
	"path"
	"time"
)

type logMessage swagger.LogMessage

func (lm *logMessage) log(status string, message string) {

	lm.Status = status
	lm.Log = message
	lm.LogTimestamp = time.Now().UTC()

	// Send to local console and file
	log.Printf("%+v\n", lm)

	usr, _ := user.Current()
	dir := usr.HomeDir
	folder := path.Join(dir, "algorun", "logs")
	if _, err := os.Stat(folder); os.IsNotExist(err) {
		os.MkdirAll(folder, os.ModePerm)
	}
	fullPathFile := path.Join(folder, fmt.Sprintf("%s.log", lm.LogMessageType))

	f, err := os.OpenFile(fullPathFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Error opening the local log file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)
	log.Printf("%+v\n", lm)

	log.SetOutput(os.Stderr)

	// Don't send undeliverable kafka errors back to Kafka
	if lm.LogMessageType != "Local" {
		// Send to Kafka
		produceLogMessage(logTopic, lm)
	}

}
