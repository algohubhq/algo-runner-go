package main

import (
	"algo-runner-go/swagger"
	"flag"
	"os"
	"strings"
	"sync"

	"github.com/go-logr/logr"

	uuid "github.com/nu7hatch/gouuid"
)

var log logr.Logger

// Global variables
var healthy bool
var instanceName *string
var kafkaBrokers *string
var config swagger.RunnerConfig
var logTopic *string

var runID string

func main() {

	healthy = false

	// Create the base log message
	localLog := logMessage{
		LogMessageType: "Local",
		Status:         "Started",
		RunnerLogData:  &swagger.RunnerLogData{},
	}

	configFilePtr := flag.String("config", "", "JSON config file to load")
	kafkaBrokersPtr := flag.String("kafka-brokers", "", "Kafka broker addresses separated by a comma")
	logTopicPtr := flag.String("log-topic", "", "Kafka topic name for logs")
	instanceNamePtr := flag.String("instance-name", "", "The Algo Instance Name (typically Container ID")

	flag.Parse()

	if *configFilePtr == "" {
		// Try to load from environment variable
		configEnv := os.Getenv("ALGO-RUNNER-CONFIG")
		if configEnv != "" {
			config = loadConfigFromString(configEnv)
		} else {
			localLog.Status = "Failed"
			localLog.RunnerLogData.Log = "Missing the config file path argument and no environment variable ALGO-RUNNER-CONFIG exists. ( --config=./config.json ) Shutting down..."
			localLog.log()

			os.Exit(1)
		}
	} else {
		config = loadConfigFromFile(*configFilePtr)
	}

	if *kafkaBrokersPtr == "" {

		// Try to load from environment variable
		kafkaBrokersEnv := os.Getenv("KAFKA-BROKERS")
		if kafkaBrokersEnv != "" {
			kafkaBrokers = &kafkaBrokersEnv
		} else {
			localLog.Status = "Failed"
			localLog.RunnerLogData.Log = "Missing the Kafka Brokers argument and no environment variable KAFKA-BROKERS exists. ( --kafka-brokers={broker1,broker2} ) Shutting down..."
			localLog.log()

			os.Exit(1)
		}

	} else {
		kafkaBrokers = kafkaBrokersPtr
	}

	if *logTopicPtr == "" {

		// Try to load from environment variable
		logTopicEnv := os.Getenv("LOG-TOPIC")
		if logTopicEnv == "" {
			logTopicEnv = "algorun.logs"
			logTopic = &logTopicEnv
		} else {
			logTopic = &logTopicEnv
		}

	} else {
		logTopic = logTopicPtr
	}

	if *instanceNamePtr == "" {

		// Try to load from environment variable
		instanceNameEnv := os.Getenv("INSTANCE-NAME")
		if instanceNameEnv == "" {
			instanceNameUUID, _ := uuid.NewV4()
			instanceNameEnv = strings.Replace(instanceNameUUID.String(), "-", "", -1)
			instanceName = &instanceNameEnv
		} else {
			instanceName = &instanceNameEnv
		}

	} else {
		instanceName = instanceNamePtr
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		startConsumers()
		wg.Done()
	}()

	createHealthHandler()

	wg.Wait()

}
