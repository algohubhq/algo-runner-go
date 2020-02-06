package main

import (
	"algo-runner-go/swagger"
	"errors"
	"flag"
	"os"
	"strings"
	"sync"

	"github.com/go-logr/logr"

	uuid "github.com/nu7hatch/gouuid"
)

// Global variables
var (
	log           logr.Logger
	healthy       bool
	instanceName  *string
	kafkaBrokers  *string
	config        swagger.AlgoRunnerConfig
	logTopic      *string
	runID         string
	execRunner    *ExecRunner
	storageConfig *StorageConfig
)

func main() {

	// Create the base log message
	localLog := logMessage{
		Type_:   "Local",
		Status:  "Started",
		Version: "1",
	}

	configFilePtr := flag.String("config", "", "JSON config file to load")
	kafkaBrokersPtr := flag.String("kafka-brokers", "", "Kafka broker addresses separated by a comma")
	logTopicPtr := flag.String("log-topic", "", "Kafka topic name for logs")
	instanceNamePtr := flag.String("instance-name", "", "The Algo Instance Name (typically Container ID")
	storagePtr := flag.String("storage-config", "", "The block storage connection string.")

	flag.Parse()

	if *configFilePtr == "" {
		// Try to load from environment variable
		configEnv := os.Getenv("ALGO-RUNNER-CONFIG")
		if configEnv != "" {
			config = loadConfigFromString(configEnv)
		} else {
			localLog.Status = "Failed"
			localLog.Msg = "Missing the config file path argument and no environment variable ALGO-RUNNER-CONFIG exists. ( --config=./config.json ) Shutting down..."
			localLog.log(errors.New("ALGO-RUNNER-CONFIG missing"))

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
			localLog.Msg = "Missing the Kafka Brokers argument and no environment variable KAFKA-BROKERS exists. ( --kafka-brokers={broker1,broker2} ) Shutting down..."
			localLog.log(errors.New("KAFKA-BROKERS missing"))

			os.Exit(1)
		}

	} else {
		kafkaBrokers = kafkaBrokersPtr
	}

	if *storagePtr == "" {

		// Try to load from environment variable
		storageEnv := os.Getenv("MC_HOST_algorun")
		if storageEnv != "" {
			storageConfig = &StorageConfig{}
			host, accessKey, secret, err := parseEnvURLStr(storageEnv)
			if err != nil {
				localLog.Status = "Failed"
				localLog.Msg = "S3 Connection String is not valid. [] Shutting down..."
				localLog.log(errors.New("S3 Connection String is not valid"))

				os.Exit(1)
			}
			storageConfig.connectionString = storageEnv
			storageConfig.host = host.Host
			storageConfig.accessKeyID = accessKey
			storageConfig.secretAccessKey = secret
			storageConfig.useSSL = host.Scheme == "https"
		} else {
			localLog.Status = "Failed"
			localLog.Msg = "Missing the Kafka Brokers argument and no environment variable KAFKA-BROKERS exists. ( --kafka-brokers={broker1,broker2} ) Shutting down..."
			localLog.log(errors.New("KAFKA-BROKERS missing"))

			os.Exit(1)
		}

	} else {
		instanceName = instanceNamePtr
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

	registerMetrics()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		startConsumers()
		wg.Done()
	}()

	if strings.ToLower(config.Executor) == "executable" ||
		strings.ToLower(config.Executor) == "delegated" {
		execRunner = newExecRunner()
	}

	createHTTPHandler()

	wg.Wait()

}
