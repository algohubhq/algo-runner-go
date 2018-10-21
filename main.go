package main

import (
	"algo-runner-go/swagger"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nu7hatch/gouuid"
)

// Global variables
var healthy bool
var healthCheckIntervalSeconds *int
var instanceName *string
var kafkaServers *string
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
	kafkaServersPtr := flag.String("kafka-servers", "", "Kafka broker addresses separated by a comma")
	logTopicPtr := flag.String("log-topic", "", "Kafka topic name for logs")
	instanceNamePtr := flag.String("instance-name", "", "The Algo Instance Name (typically Container ID")
	healthCheckIntervalSecondsPtr := flag.Int("health-check-interval", 0, "Interval for the health check to write to the health file. (in seconds)")

	flag.Parse()

	if *healthCheckIntervalSecondsPtr == 0 {
		// Try to load from environment variable
		healthCheckIntervalSecondsEnv := os.Getenv("HEALTH-CHECK-INTERVAL")
		if healthCheckIntervalSecondsEnv != "" {
			var intErr error
			healthCheckIntervalSecondsParse, intErr := strconv.Atoi(healthCheckIntervalSecondsEnv)
			if intErr != nil {
				// Default the health check to 30 seconds
				healthCheckIntervalSecondsParse = 30
			}
			healthCheckIntervalSeconds = &healthCheckIntervalSecondsParse
		} else {
			// Default the health check to 30 seconds
			healthCheckIntervalSecondsParse := 30
			healthCheckIntervalSeconds = &healthCheckIntervalSecondsParse
		}
	} else {
		healthCheckIntervalSeconds = healthCheckIntervalSecondsPtr
	}

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

	if *kafkaServersPtr == "" {

		// Try to load from environment variable
		kafkaServersEnv := os.Getenv("KAFKA-SERVERS")
		if kafkaServersEnv != "" {
			kafkaServers = &kafkaServersEnv
		} else {
			localLog.Status = "Failed"
			localLog.RunnerLogData.Log = "Missing the Kafka Servers argument and no environment variable KAFKA-SERVERS exists. ( --kafka-servers={broker1,broker2} ) Shutting down..."
			localLog.log()

			os.Exit(1)
		}

	} else {
		kafkaServers = kafkaServersPtr
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

	// Launch the server if not started
	if strings.ToLower(config.ServerType) != "serverless" {

		var serverTerminated bool
		go func() {
			serverTerminated = startServer()
			if serverTerminated {
				healthy = false
				os.Exit(1)
			}
		}()

	}

	startReadinessLivenessTouch(time.Duration(*healthCheckIntervalSeconds) * time.Second)

	startConsumers()

}

func startReadinessLivenessTouch(d time.Duration) {

	go func() {

		var filename string
		var createErr error
		// Only write the health file once healthy
		if healthy {
			filename, createErr = createHealthFile()
			if createErr != nil {
				// TODO: log the error that the tmp health file can't be created
			}
		}

		for x := range time.Tick(d) {
			log.Printf("Health loop tick. Healthy: %t\n", healthy)
			if healthy {
				filename, createErr = createHealthFile()
				if createErr != nil {
					// TODO: log the error that the tmp health file can't be created
				}
				if err := os.Chtimes(filename, x, x); err != nil {
					log.Fatal(err)
				}
			}
		}

	}()

}

func createHealthFile() (string, error) {

	path := filepath.Join(os.TempDir(), ".health")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Printf("Writing health file to: %s\n", path)
		writeErr := ioutil.WriteFile(path, []byte{}, 0660)
		return path, writeErr
	}

	return path, nil
}
