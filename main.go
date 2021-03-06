package main

import (
	configloader "algo-runner-go/pkg/config"
	k "algo-runner-go/pkg/kafka"
	kafkaconsumer "algo-runner-go/pkg/kafka/consumer"
	kafkaproducer "algo-runner-go/pkg/kafka/producer"
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	"algo-runner-go/pkg/storage"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	uuid "github.com/nu7hatch/gouuid"
)

// Global variables
var (
	config                  openapi.AlgoRunnerConfig
	instanceName            string
	kafkaBrokers            string
	storageConnectionString string
)

func main() {

	// Create the local logger
	logType := openapi.LOGTYPES_RUNNER
	localLogger := logging.NewLogger(
		&openapi.LogEntryModel{
			Type:    &logType,
			Version: "1",
		},
		nil)

	healthyChan := make(chan bool)

	// We need to shut down gracefully when the user hits Ctrl-C.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGTERM, syscall.SIGHUP)

	configLoader := configloader.NewConfigLoader(&localLogger)

	configFilePtr := flag.String("config", "", "JSON config file to load")
	kafkaBrokersPtr := flag.String("kafka-brokers", "", "Kafka broker addresses separated by a comma")
	instanceNamePtr := flag.String("instance-name", "", "The Algo Instance Name (typically Container ID")
	storagePtr := flag.String("storage-config", "", "The block storage connection string.")

	flag.Parse()

	if *configFilePtr == "" {
		// Try to load from environment variable
		configEnv := os.Getenv("ALGO_RUNNER_CONFIG")
		if configEnv != "" {
			config = configLoader.LoadConfigFromString(configEnv)
		} else {
			localLogger.Error("Missing the config file path argument and no environment variable ALGO_RUNNER_CONFIG exists. ( --config=./config.json ) Shutting down...",
				errors.New("ALGO_RUNNER_CONFIG missing"))

			os.Exit(1)
		}
	} else {
		config = configLoader.LoadConfigFromFile(*configFilePtr)
	}

	if *kafkaBrokersPtr == "" {

		// Try to load from environment variable
		kafkaBrokersEnv := os.Getenv("KAFKA_BROKERS")
		if kafkaBrokersEnv != "" {
			kafkaBrokers = kafkaBrokersEnv
		} else {
			localLogger.Error("Missing the Kafka Brokers argument and no environment variable KAFKA-BROKERS exists. ( --kafka-brokers={broker1,broker2} ) Shutting down...",
				errors.New("KAFKA_BROKERS missing"))

			os.Exit(1)
		}

	} else {
		kafkaBrokers = *kafkaBrokersPtr
	}

	if *storagePtr == "" {
		// Try to load from environment variable
		storageEnv := os.Getenv("MC_HOST_algorun")
		if storageEnv != "" {
			storageConnectionString = storageEnv
		} else {
			localLogger.Error("Missing the S3 Storage Connection String argument and no environment variable MC_HOST_algorun exists.",
				errors.New("MC_HOST_algorun missing"))
		}
	}

	if *instanceNamePtr == "" {

		// Try to load from environment variable
		instanceNameEnv := os.Getenv("INSTANCE_NAME")
		if instanceNameEnv == "" {
			instanceNameUUID, _ := uuid.NewV4()
			instanceNameEnv = strings.Replace(instanceNameUUID.String(), "-", "", -1)
			instanceName = instanceNameEnv
		} else {
			instanceName = instanceNameEnv
		}

	} else {
		instanceName = *instanceNamePtr
	}

	// Create the runner logger
	runnerLogger := logging.NewLogger(
		&openapi.LogEntryModel{
			Type:    &logType,
			Version: "1",
			Data: map[string]interface{}{
				"DeploymentOwner":  config.DeploymentOwner,
				"DeploymentName":   config.DeploymentName,
				"AlgoOwner":        config.Owner,
				"AlgoName":         config.Name,
				"AlgoVersionTag":   config.Version,
				"AlgoIndex":        config.Index,
				"AlgoInstanceName": instanceName,
			},
		},
		nil)

	metrics := metrics.NewMetrics(healthyChan, &config)

	var storageConfig *storage.Storage
	if storageConnectionString != "" {
		storageConfig = storage.NewStorage(healthyChan, &config, storageConnectionString, &runnerLogger)
	} else {
		// Check if there are any pipes with File Reference, which require a storage connection
		for _, pipe := range config.Pipes {
			if *pipe.SourceOutputMessageDataType == openapi.MESSAGEDATATYPES_FILE_REFERENCE {
				localLogger.Error(
					fmt.Sprintf("Pipe Message Data Type is set to file reference but there is no storage connection string. Pipe name: [%s/%s] Shutting down...", pipe.SourceName, pipe.SourceOutputName),
					errors.New("Failed to setup storage connection"))
				os.Exit(1)
			}
		}
	}

	kafkaConfig := k.NewKafkaConfig(&config, kafkaBrokers)

	producer, err := kafkaproducer.NewProducer(healthyChan, &config, kafkaConfig, instanceName, &runnerLogger, &metrics)
	if err != nil {
		localLogger.Error("Failed to create Kafka Producer... Shutting down...",
			errors.New("Failed to create Kafka Producer"))
		os.Exit(1)
	}

	go metrics.CreateHTTPHandler()

	// Start Consumers
	go func() {
		consumers, err := kafkaconsumer.NewConsumers(healthyChan,
			&config,
			kafkaConfig,
			producer,
			storageConfig,
			instanceName,
			&runnerLogger,
			&metrics)

		if err == nil {
			consumers.Start()
		}
	}()

	for {
		signal := <-sig
		switch signal {
		case syscall.SIGTERM, syscall.SIGINT:
			// Try to gracefully shutdown
			producer.KafkaProducer.Close()
		}
	}

}
