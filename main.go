package main

import (
	"algo-runner-go/swagger"
	"flag"
	"os"
	"strings"
)

// Global variables
var kafkaServers string
var config swagger.RunnerConfig
var logTopic string

var runID string

func main() {

	// Create the base log message
	localLog := logMessage{
		LogMessageType: "Local",
		Status:         "Started",
	}

	logTopic = "algorun.runner.logs"

	configFilePtr := flag.String("config", "./config.json", "JSON config file to load")
	kafkaServersPtr := flag.String("kafka-servers", "localhost:9092", "Kafka broker addresses separated by a comma")

	flag.Parse()

	if *configFilePtr == "" {
		localLog.log("Failed", "Missing the config file path argument. ( --config=./config.json ) Shutting down...")
		os.Exit(1)
	}

	config = loadConfig(*configFilePtr)

	if *kafkaServersPtr != "" {
		kafkaServers = *kafkaServersPtr
	} else {
		localLog.log("Failed", "Missing the Kafka Servers argument. ( --kafka-servers={broker1,broker2} ) Shutting down...")
		os.Exit(1)
	}

	// Launch the server if not started
	if strings.ToLower(config.ServerType) != "serverless" {

		var serverTerminated bool
		go func() {
			serverTerminated = startServer()
			if serverTerminated {
				os.Exit(1)
			}
		}()

	}

	startConsumers()

}
