package kafka

import (
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/minio/minio-go"
	uuid "github.com/nu7hatch/gouuid"
)

type Consumer struct {
	Config       *openapi.AlgoRunnerConfig
	Logger       *logging.Logger
	Metrics      *metrics.Metrics
	InstanceName string
}

// NewConsumer returns a new Consumer struct
func NewConsumer(config *openapi.AlgoRunnerConfig,
	logger *logging.Logger,
	metrics *metrics.Metrics) Consumer {

	return Consumer{
		Config:  config,
		Logger:  logger,
		Metrics: metrics,
	}
}

type topicInputs map[string]*openapi.AlgoInputModel

func (c *Consumer) StartConsumers() {

	// Create the base log message
	runnerLog := openapi.LogEntryModel{
		Type:    "Runner",
		Version: "1",
		Data: map[string]interface{}{
			"DeploymentOwnerUserName": c.Config.DeploymentOwnerUserName,
			"DeploymentName":          c.Config.DeploymentName,
			"AlgoOwnerUserName":       c.Config.AlgoOwnerUserName,
			"AlgoName":                c.Config.AlgoName,
			"AlgoVersionTag":          c.Config.AlgoVersionTag,
			"AlgoInstanceName":        c.InstanceName,
		},
	}

	topicInputs := make(topicInputs)
	var topics []string
	algoName := fmt.Sprintf("%s/%s:%s[%d]", c.Config.AlgoOwnerUserName, c.Config.AlgoName, c.Config.AlgoVersionTag, c.Config.AlgoIndex)

	for _, pipe := range c.Config.Pipes {

		if pipe.DestName == algoName {

			var input openapi.AlgoInputModel
			// Get the input associated with this route
			for i := range c.Config.Inputs {
				if c.Config.Inputs[i].Name == pipe.DestInputName {
					input = c.Config.Inputs[i]
					break
				}
			}

			var topicConfig openapi.TopicConfigModel
			// Get the topic c.Config associated with this route
			for x := range c.Config.TopicConfigs {
				if c.Config.TopicConfigs[x].SourceName == pipe.SourceName &&
					c.Config.TopicConfigs[x].SourceOutputName == pipe.SourceOutputName {
					topicConfig = c.Config.TopicConfigs[x]
					break
				}
			}

			// Replace the deployment username and name in the topic string
			topicName := strings.ToLower(strings.Replace(topicConfig.TopicName, "{deploymentownerusername}", c.Config.DeploymentOwnerUserName, -1))
			topicName = strings.ToLower(strings.Replace(topicName, "{deploymentname}", c.Config.DeploymentName, -1))

			topicInputs[topicName] = &input
			topics = append(topics, topicName)

			runnerLog.Msg = fmt.Sprintf("Listening to topic %s", topicName)
			runnerLog.Log(nil)

		}

	}

	groupID := fmt.Sprintf("algorun-%s-%s-%s-%s-%d-dev",
		c.Config.DeploymentOwnerUserName,
		c.Config.DeploymentName,
		c.Config.AlgoOwnerUserName,
		c.Config.AlgoName,
		c.Config.AlgoIndex,
	)

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers":        *kafkaBrokers,
		"group.id":                 groupID,
		"client.id":                "algo-runner-go-client",
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
		"auto.offset.reset":        "earliest",
	}

	// Set the ssl c.Config if enabled
	if CheckForKafkaTLS() {
		kafkaConfig["security.protocol"] = "ssl"
		kafkaConfig["ssl.ca.location"] = kafkaTLSCaLocation
		kafkaConfig["ssl.certificate.location"] = kafkaTLSUserLocation
		kafkaConfig["ssl.key.location"] = kafkaTLSKeyLocation
	}

	kc, err := kafka.NewConsumer(&kafkaConfig)

	if err != nil {
		healthy = false
		runnerLog.Msg = fmt.Sprintf("Failed to create consumer.")
		runnerLog.Log(err)

		os.Exit(1)
	}

	err = kc.SubscribeTopics(topics, nil)

	c.waitForMessages(kc, topicInputs)

}

func (c *Consumer) waitForMessages(kc *kafka.Consumer, topicInputs topicInputs) {

	// Create the base log message
	runnerLog := openapi.LogEntryModel{
		Type:    "Runner",
		Version: "1",
		Data: map[string]interface{}{
			"DeploymentOwnerUserName": c.Config.DeploymentOwnerUserName,
			"DeploymentName":          c.Config.DeploymentName,
			"AlgoOwnerUserName":       c.Config.AlgoOwnerUserName,
			"AlgoName":                c.Config.AlgoName,
			"AlgoVersionTag":          c.Config.AlgoVersionTag,
			"AlgoInstanceName":        c.InstanceName,
		},
	}

	defer kc.Close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	data := make(map[string]map[*openapi.AlgoInputModel][]InputData)

	offsets := make(map[string]kafka.TopicPartition)

	waiting := true
	firstPoll := true

	for waiting == true {
		select {
		case sig := <-sigchan:

			runnerLog.Msg = fmt.Sprintf("Caught signal %v: terminating the Kafka Consumer process.", sig)
			runnerLog.Log(errors.New("Terminating"))

			healthy = false
			waiting = false

		default:

			var ev kafka.Event
			if firstPoll {
				ev = kc.Poll(100)
			} else {
				ev = kc.Poll(-1)
			}

			if ev == nil {
				if firstPoll {
					healthy = true
					firstPoll = false
				}
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:

				healthy = true

				startTime := time.Now()

				runnerLog.Msg = fmt.Sprintf("Kafka Message received on %s", e.TopicPartition)
				runnerLog.Log(nil)

				input := topicInputs[*e.TopicPartition.Topic]
				traceID, inputData, run, endpointParams := c.processMessage(e, input)

				if data[traceID] == nil {
					data[traceID] = make(map[*openapi.AlgoInputModel][]InputData)
				}

				data[traceID][input] = append(data[traceID][input], inputData)

				if run {

					// TODO: iterate over inputs to be sure at least one has data!
					// Can check to be sure all required inputs are fulfilled as well

					var runError error

					switch executor := c.Config.Executor; executor {
					case openapi.EXECUTORS_EXECUTABLE, openapi.EXECUTORS_DELEGATED:
						runError = execRunner.run(traceID, endpointParams, data[traceID])
					case openapi.EXECUTORS_HTTP:
						runError = runHTTP(traceID, endpointParams, data[traceID])
					case openapi.EXECUTORS_GRPC:
						runError = errors.New("gRPC executor is not implemented")
					case openapi.EXECUTORS_SPARK:
						runError = errors.New("Spark executor is not implemented")
					default:
						// Not implemented
						runError = errors.New("Unknown executor is not supported")
					}

					if runError == nil {

						// Increment the offset
						// Store the offset and commit
						offsetCommit := kafka.TopicPartition{
							Topic:     e.TopicPartition.Topic,
							Partition: e.TopicPartition.Partition,
							Offset:    e.TopicPartition.Offset + 1,
						}

						offsets[traceID] = offsetCommit

						_, offsetErr := kc.StoreOffsets([]kafka.TopicPartition{offsets[traceID]})
						if offsetErr != nil {
							runnerLog.Msg = fmt.Sprintf("Failed to store offsets for [%v]",
								[]kafka.TopicPartition{offsets[traceID]})
							runnerLog.Log(offsetErr)
						}

						_, commitErr := kc.Commit()
						if commitErr != nil {
							runnerLog.Msg = fmt.Sprintf("Failed to commit offsets.")
							runnerLog.Log(commitErr)
						}

						delete(data, traceID)
						delete(offsets, traceID)

					} else {
						runnerLog.Msg = "Failed to run algo"
						runnerLog.Log(runError)
					}

					reqDuration := time.Since(startTime)
					runnerRuntimeHistogram.WithLabelValues(deploymentLabel, pipelineLabel, componentLabel, algoLabel, algoVersionLabel, algoIndexLabel).Observe(reqDuration.Seconds())

				} else {
					// Save the offset for the data that was only stored but not executed
					// Will be committed after successful run
					offsets[traceID] = e.TopicPartition
				}

			case kafka.AssignedPartitions:
				healthy = true
				runnerLog.Msg = fmt.Sprintf("%v", e)
				runnerLog.Log(nil)
				kc.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				runnerLog.Msg = fmt.Sprintf("%v", e)
				runnerLog.Log(nil)
				kc.Unassign()
			case kafka.Error:
				runnerLog.Msg = fmt.Sprintf("Kafka Error: %v", e)
				runnerLog.Log(nil)
				healthy = false
				waiting = false

			}
		}
	}

	healthy = false

}

func (c *Consumer) processMessage(msg *kafka.Message,
	input *openapi.AlgoInputModel) (traceID string, inputData InputData, run bool, endpointParams string) {

	// Default to run - if header is set to false, then don't run
	run = true
	// traceID is the message key
	traceID = string(msg.Key)

	// Create the base log message
	runnerLog := openapi.LogEntryModel{
		Type:    "Runner",
		Version: "1",
		Data: map[string]interface{}{
			"DeploymentOwnerUserName": c.Config.DeploymentOwnerUserName,
			"DeploymentName":          c.Config.DeploymentName,
			"AlgoOwnerUserName":       c.Config.AlgoOwnerUserName,
			"AlgoName":                c.Config.AlgoName,
			"AlgoVersionTag":          c.Config.AlgoVersionTag,
			"AlgoInstanceName":        c.InstanceName,
		},
	}

	// Parse the headers
	var contentType string
	var messageDataType openapi.MessageDataTypes
	var fileName string
	for _, header := range msg.Headers {
		switch header.Key {
		case "contentType":
			contentType = string(header.Value)
		case "fileName":
			fileName = string(header.Value)
		case "messageDataType":
			messageDataType.UnmarshalText(header.Value)
		case "endpointParams":
			endpointParams = string(header.Value)
		case "run":
			b, _ := strconv.ParseBool(string(header.Value))
			run = b
		}
	}

	if traceID == "" {
		uuidTraceID, _ := uuid.NewV4()
		traceID = strings.Replace(uuidTraceID.String(), "-", "", -1)
	}

	// If the content type is empty, use the first accepted content type
	if contentType == "" {
		if len(input.ContentTypes) > 0 {
			contentType = input.ContentTypes[0].Name
		}
	}

	// Check if the content is empty then this message is to trigger a run only
	if run && len(msg.Value) < 1 {
		return
	}

	// TODO: Validate the content type

	// Save the data based on the delivery type
	inputData = InputData{}
	inputData.contentType = contentType

	// These input delivery types expect a byte stream
	if input.InputDeliveryType == openapi.INPUTDELIVERYTYPES_STD_IN ||
		input.InputDeliveryType == openapi.INPUTDELIVERYTYPES_HTTP ||
		input.InputDeliveryType == openapi.INPUTDELIVERYTYPES_HTTPS {

		// If messageDataType is file reference then load file
		if messageDataType == openapi.MESSAGEDATATYPES_FILE_REFERENCE {
			// Try to read the json
			var fileReference openapi.FileReference
			jsonErr := json.Unmarshal(msg.Value, &fileReference)

			if jsonErr != nil {
				runnerLog.Msg = fmt.Sprintf("Failed to parse the FileReference json.")
				runnerLog.Log(jsonErr)
			}

			// Read the file from storage
			// Initialize minio client object.
			minioClient, err := minio.New(storageConfig.host, storageConfig.accessKeyID, storageConfig.secretAccessKey, storageConfig.useSSL)
			if err != nil {
				runnerLog.Msg = fmt.Sprintf("Failed to create minio client.")
				runnerLog.Log(err)
			}
			object, err := minioClient.GetObject(fileReference.Bucket, fileReference.File, minio.GetObjectOptions{})
			if err != nil {
				runnerLog.Msg = fmt.Sprintf("Failed to get file reference object from storage. [%v]", fileReference)
				runnerLog.Log(err)
			}

			objectBytes, err := ioutil.ReadAll(object)
			if err != nil {
				runnerLog.Msg = fmt.Sprintf("Failed to read file reference bytes from storage. [%v]", fileReference)
				runnerLog.Log(err)
			}

			inputData.isFileReference = false
			inputData.data = objectBytes

		} else {
			// If the data is embedded then copy the message value
			inputData.isFileReference = false
			inputData.data = msg.Value
		}

	} else {
		// These input delivery types expect a file

		// If messageDataType is file reference then ensure file exists and convert to container path
		if messageDataType == openapi.MESSAGEDATATYPES_FILE_REFERENCE {

			// Write the file locally
			inputData.isFileReference = true
			// Try to read the json
			var fileReference openapi.FileReference
			jsonErr := json.Unmarshal(msg.Value, &fileReference)

			if jsonErr != nil {
				runnerLog.Msg = fmt.Sprintf("Failed to parse the FileReference json.")
				runnerLog.Log(jsonErr)
			}

			// Read the file from storage
			// Initialize minio client object.
			minioClient, err := minio.New(storageConfig.host, storageConfig.accessKeyID, storageConfig.secretAccessKey, storageConfig.useSSL)
			if err != nil {
				runnerLog.Msg = fmt.Sprintf("Failed to create minio client.")
				runnerLog.Log(err)
			}
			object, err := minioClient.GetObject(fileReference.Bucket, fileReference.File, minio.GetObjectOptions{})
			if err != nil {
				runnerLog.Msg = fmt.Sprintf("Failed to get file reference object from storage. [%v]", fileReference)
				runnerLog.Log(err)
			}

			filePath := path.Join("/input", fileReference.File)
			localFile, err := os.Create(filePath)
			if err != nil {
				runnerLog.Msg = fmt.Sprintf("Failed to create local file from reference object. [%v]", fileReference)
				runnerLog.Log(err)
			}
			if _, err = io.Copy(localFile, object); err != nil {
				runnerLog.Msg = fmt.Sprintf("Failed to copy byte stream from reference object to local file. [%v]", fileReference)
				runnerLog.Log(err)
			}

			inputData.fileReference = &fileReference

		} else {

			// The data is embedded so write the file locally as the algo expects a file
			inputData.isFileReference = true
			if fileName == "" {
				fileName = traceID
			}

			fullPathFile := path.Join("/input", fileName)

			fileReference := openapi.FileReference{
				File: fileName,
			}

			err := ioutil.WriteFile(fullPathFile, msg.Value, 0644)
			if err != nil {
				runnerLog.Msg = fmt.Sprintf("Unable to write the embedded data to file [%s]", fullPathFile)
				runnerLog.Log(err)
			}

			inputData.fileReference = &fileReference

		}
	}

	bytesProcessedCounter.WithLabelValues(deploymentLabel, pipelineLabel, componentLabel, algoLabel, algoVersionLabel, algoIndexLabel).Add(float64(binary.Size(msg.Value)))

	return

}
