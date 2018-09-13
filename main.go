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

	configFilePtr := flag.String("config", "", "JSON config file to load")
	kafkaServersPtr := flag.String("kafka-servers", "", "Kafka broker addresses separated by a comma")

	flag.Parse()

	if *configFilePtr == "" {
		// Try to load from environment variable
		configEnv := os.Getenv("ALGO-RUNNER-CONFIG")
		if configEnv != "" {
			config = loadConfigFromString(configEnv)
		} else {
			localLog.log("Failed", "Missing the config file path argument and no environment variable ALGO-RUNNER-CONFIG exists. ( --config=./config.json ) Shutting down...")
			os.Exit(1)
		}
	} else {
		config = loadConfigFromFile(*configFilePtr)
	}

	if *kafkaServersPtr == "" {

		// Try to load from environment variable
		kafkaServerEnv := os.Getenv("KAFKA-SERVERS")
		if kafkaServerEnv != "" {
			kafkaServersPtr = &kafkaServerEnv
			kafkaServers = *kafkaServersPtr
		} else {
			localLog.log("Failed", "Missing the Kafka Servers argument and no environment variable KAFKA-SERVERS exists. ( --kafka-servers={broker1,broker2} ) Shutting down...")
			os.Exit(1)
		}

	} else {
		kafkaServers = *kafkaServersPtr
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
