package main

import (
	configloader "algo-runner-go/pkg/config"
	kafkaconsumer "algo-runner-go/pkg/kafka/consumer"
	kafkaproducer "algo-runner-go/pkg/kafka/producer"
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	"algo-runner-go/pkg/types"
	"errors"
	"flag"
	"os"
	"strings"
	"sync"

	uuid "github.com/nu7hatch/gouuid"
)

// Global variables
var (
	config        openapi.AlgoRunnerConfig
	storageConfig types.StorageConfig
	instanceName  string
	kafkaBrokers  string
)

func main() {

	// Create the local logger
	localLogger := logging.NewLogger(
		&openapi.LogEntryModel{
			Type:    "Local",
			Version: "1",
		},
		nil)

	healthyChan := make(chan bool)

	configLoader := configloader.NewConfigLoader(&localLogger)

	configFilePtr := flag.String("config", "", "JSON config file to load")
	kafkaBrokersPtr := flag.String("kafka-brokers", "", "Kafka broker addresses separated by a comma")
	instanceNamePtr := flag.String("instance-name", "", "The Algo Instance Name (typically Container ID")
	storagePtr := flag.String("storage-config", "", "The block storage connection string.")

	flag.Parse()

	if *configFilePtr == "" {
		// Try to load from environment variable
		configEnv := os.Getenv("ALGO-RUNNER-CONFIG")
		if configEnv != "" {
			config = configLoader.LoadConfigFromString(configEnv)
		} else {
			localLogger.LogMessage.Msg = "Missing the config file path argument and no environment variable ALGO-RUNNER-CONFIG exists. ( --config=./config.json ) Shutting down..."
			localLogger.Log(errors.New("ALGO-RUNNER-CONFIG missing"))

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
			localLogger.LogMessage.Msg = "Missing the Kafka Brokers argument and no environment variable KAFKA-BROKERS exists. ( --kafka-brokers={broker1,broker2} ) Shutting down..."
			localLogger.Log(errors.New("KAFKA_BROKERS missing"))

			os.Exit(1)
		}

	} else {
		kafkaBrokers = *kafkaBrokersPtr
	}

	if *storagePtr == "" {
		// Try to load from environment variable
		storageEnv := os.Getenv("MC_HOST_algorun")
		if storageEnv != "" {
			storageConfig = types.StorageConfig{}
			host, accessKey, secret, err := configLoader.ParseEnvURLStr(storageEnv)
			if err != nil {
				localLogger.LogMessage.Msg = "S3 Connection String is not valid. [] Shutting down..."
				localLogger.Log(errors.New("S3 Connection String is not valid"))

				os.Exit(1)
			}
			storageConfig.ConnectionString = storageEnv
			storageConfig.Host = host.Host
			storageConfig.AccessKeyID = accessKey
			storageConfig.SecretAccessKey = secret
			storageConfig.UseSSL = host.Scheme == "https"
		} else {
			localLogger.LogMessage.Msg = "Missing the S3 Storage Connection String argument and no environment variable MC_HOST_algorun exists."
			localLogger.Log(errors.New("MC_HOST_algorun missing"))
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
			Type:    "Runner",
			Version: "1",
			Data: map[string]interface{}{
				"DeploymentOwnerUserName": config.DeploymentOwnerUserName,
				"DeploymentName":          config.DeploymentName,
				"AlgoOwnerUserName":       config.AlgoOwnerUserName,
				"AlgoName":                config.AlgoName,
				"AlgoVersionTag":          config.AlgoVersionTag,
				"AlgoIndex":               config.AlgoIndex,
				"AlgoInstanceName":        instanceName,
			},
		},
		nil)

	metrics := metrics.NewMetrics(healthyChan, &config)

	var wg sync.WaitGroup
	wg.Add(1)

	producer := kafkaproducer.NewProducer(healthyChan, &config, instanceName, kafkaBrokers, &runnerLogger, &metrics)

	go func() {
		consumer := kafkaconsumer.NewConsumer(healthyChan,
			&config,
			&producer,
			&storageConfig,
			instanceName,
			kafkaBrokers,
			&runnerLogger,
			&metrics)
		consumer.StartConsumers()
		wg.Done()
	}()

	metrics.CreateHTTPHandler()

	wg.Wait()

}
