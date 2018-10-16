package main

import (
	"algo-runner-go/swagger"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/user"
	"path"
	"time"
)

type logMessage swagger.LogMessage

func (lm *logMessage) log() {

	lm.LogTimestamp = time.Now().UTC()

	// Send to local console and file
	lmBytes, err := json.Marshal(lm)
	log.Printf("%s\n", string(lmBytes))

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
	log.Printf("%s\n", string(lmBytes))

	log.SetOutput(os.Stderr)

	// Don't send undeliverable kafka errors back to Kafka
	if lm.LogMessageType != "Local" {
		// Send to Kafka
		produceLogMessage(lmBytes)
	}

}
