package kafkaconsumer

import (
	kafkaproducer "algo-runner-go/pkg/kafka/producer"
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	"algo-runner-go/pkg/runner"
	"algo-runner-go/pkg/storage"
	"algo-runner-go/pkg/types"
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
	HealthyChan       chan<- bool
	config            *openapi.AlgoRunnerConfig
	KafkaConsumer     *kafka.Consumer
	Producer          *kafkaproducer.Producer
	storageConfig     *storage.Storage
	logger            *logging.Logger
	metrics           *metrics.Metrics
	instanceName      string
	kafkaBrokers      string
	baseTopic         string
	topic             string
	runner            *runner.Runner
	input             *openapi.AlgoInputModel
	retryStrategy     *openapi.TopicRetryStrategyModel
	assignedRetryStep *openapi.TopicRetryStepModel
	offsets           map[string]kafka.TopicPartition
}

// NewConsumer returns a new Consumer struct
func NewConsumer(healthyChan chan<- bool,
	config *openapi.AlgoRunnerConfig,
	kafkaConsumer *kafka.Consumer,
	input *openapi.AlgoInputModel,
	retryStrategy *openapi.TopicRetryStrategyModel,
	retryStep *openapi.TopicRetryStepModel,
	producer *kafkaproducer.Producer,
	storageConfig *storage.Storage,
	instanceName string,
	kafkaBrokers string,
	baseTopic string,
	topic string,
	logger *logging.Logger,
	metrics *metrics.Metrics) Consumer {

	r := runner.NewRunner(config,
		producer,
		storageConfig,
		instanceName,
		kafkaBrokers,
		logger,
		metrics)

	return Consumer{
		HealthyChan:       healthyChan,
		config:            config,
		KafkaConsumer:     kafkaConsumer,
		input:             input,
		retryStrategy:     retryStrategy,
		assignedRetryStep: retryStep,
		Producer:          producer,
		storageConfig:     storageConfig,
		logger:            logger,
		metrics:           metrics,
		instanceName:      instanceName,
		kafkaBrokers:      kafkaBrokers,
		baseTopic:         baseTopic,
		topic:             topic,
		runner:            &r,
	}
}

func (c *Consumer) Start() error {

	err := c.KafkaConsumer.Subscribe(c.topic, nil)
	if err != nil {
		c.HealthyChan <- false
		c.logger.LogMessage.Msg = fmt.Sprintf("Failed to subscribe to topics.")
		c.logger.Log(err)
		return err
	}

	c.logger.LogMessage.Msg = fmt.Sprintf("Listening to topic %s", c.topic)
	c.logger.Log(nil)

	go c.pollForMessages()

	return nil

}

func (c *Consumer) pollForMessages() {

	defer c.KafkaConsumer.Close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	data := make(map[string]map[*openapi.AlgoInputModel][]types.InputData)

	c.offsets = make(map[string]kafka.TopicPartition)

	waiting := true
	firstPoll := true

	for waiting == true {
		select {
		case sig := <-sigchan:

			c.logger.LogMessage.Msg = fmt.Sprintf("Caught signal %v: terminating the Kafka Consumer process.", sig)
			c.logger.Log(errors.New("Terminating"))

			c.HealthyChan <- false
			waiting = false

		default:

			var ev kafka.Event
			if firstPoll {
				ev = c.KafkaConsumer.Poll(100)
			} else {
				ev = c.KafkaConsumer.Poll(-1)
			}

			if ev == nil {
				if firstPoll {
					c.HealthyChan <- true
					firstPoll = false
				}
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:

				c.HealthyChan <- true
				c.logger.LogMessage.TraceId = ""

				startTime := time.Now()

				c.logger.LogMessage.Msg = fmt.Sprintf("Kafka Message received on %s", e.TopicPartition)
				c.logger.Log(nil)

				processedMsg := c.processMessage(e, c.input)

				if data[processedMsg.TraceID] == nil {
					data[processedMsg.TraceID] = make(map[*openapi.AlgoInputModel][]types.InputData)
				}

				data[processedMsg.TraceID][c.input] = append(data[processedMsg.TraceID][c.input], processedMsg.InputData)

				if processedMsg.Run {

					// TODO: iterate over inputs to be sure at least one has data!
					// Can check to be sure all required inputs are fulfilled as well

					c.run(processedMsg, e, data[processedMsg.TraceID])

					// Increment the offset
					// Store the offset and commit
					offsetCommit := kafka.TopicPartition{
						Topic:     e.TopicPartition.Topic,
						Partition: e.TopicPartition.Partition,
						Offset:    e.TopicPartition.Offset + 1,
					}

					c.offsets[processedMsg.TraceID] = offsetCommit

					_, offsetErr := c.KafkaConsumer.StoreOffsets([]kafka.TopicPartition{c.offsets[processedMsg.TraceID]})
					if offsetErr != nil {
						c.logger.LogMessage.Msg = fmt.Sprintf("Failed to store offsets for [%v]",
							[]kafka.TopicPartition{c.offsets[processedMsg.TraceID]})
						c.logger.Log(offsetErr)
					}

					_, commitErr := c.KafkaConsumer.Commit()
					if commitErr != nil {
						c.logger.LogMessage.Msg = fmt.Sprintf("Failed to commit offsets.")
						c.logger.Log(commitErr)
					}

					delete(data, processedMsg.TraceID)
					delete(c.offsets, processedMsg.TraceID)

					reqDuration := time.Since(startTime)
					c.metrics.RunnerRuntimeHistogram.WithLabelValues(c.metrics.DeploymentLabel,
						c.metrics.PipelineLabel,
						c.metrics.ComponentLabel,
						c.metrics.AlgoLabel,
						c.metrics.AlgoVersionLabel,
						c.metrics.AlgoIndexLabel).Observe(reqDuration.Seconds())

				} else {
					// Save the offset for the data that was only stored but not executed
					// Will be committed after successful run
					c.offsets[processedMsg.TraceID] = e.TopicPartition
				}

			case kafka.AssignedPartitions:
				c.HealthyChan <- true
				c.logger.LogMessage.Msg = fmt.Sprintf("%v", e)
				c.logger.Log(nil)
				c.KafkaConsumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				c.logger.LogMessage.Msg = fmt.Sprintf("%v", e)
				c.logger.Log(nil)
				c.KafkaConsumer.Unassign()
			case kafka.Error:
				c.logger.LogMessage.Msg = fmt.Sprintf("Kafka Error: %v", e)
				c.logger.Log(nil)
				c.HealthyChan <- false
				waiting = false

			}
		}
	}

	c.HealthyChan <- false

}

func (c *Consumer) run(processedMsg *types.ProcessedMsg,
	rawMessage *kafka.Message,
	inputData map[*openapi.AlgoInputModel][]types.InputData) {

	if c.config.TopicRetryEnabled &&
		c.config.RetryStrategy != nil &&
		*c.config.RetryStrategy.Strategy == openapi.RETRYSTRATEGIES_RETRY_TOPICS &&
		processedMsg.RetryTimestamp != nil {
		// Sleep for the message timestamp duration as this a retry consumer

		sleepTime := -time.Since(*processedMsg.RetryTimestamp)
		// Sleep the thread for the duration
		time.Sleep(sleepTime)

	}

	runError := c.runner.Run(processedMsg.TraceID, processedMsg.EndpointParams, inputData)
	if runError != nil {
		c.setInputDataMetrics("error", processedMsg)
		if c.config.TopicRetryEnabled {
			c.retry(processedMsg, rawMessage, inputData)
		} else {
			c.logger.LogMessage.Msg = "Failed to run algo. Retries Disabled."
			c.logger.Log(runError)
		}
	}

	c.setInputDataMetrics("ok", processedMsg)

}

func (c *Consumer) retry(processedMsg *types.ProcessedMsg,
	rawMessage *kafka.Message,
	inputData map[*openapi.AlgoInputModel][]types.InputData) {

	// Get the step duration
	var step openapi.TopicRetryStepModel
	retry := false
	for _, s := range c.retryStrategy.Steps {
		// If retry timestamp is nil then it's the first retry
		if processedMsg.RetryTimestamp == nil {
			step = s
			retry = true
			break
		}
		// If we are in a repeat cycle, repeat the same step
		if processedMsg.RetryStepIndex == int(s.Index) &&
			processedMsg.RetryNum < int(s.Repeat) {
			step = s
			retry = true
			break
		}
		// Otherwise get the next step
		if processedMsg.RetryStepIndex < int(s.Index) &&
			processedMsg.RetryNum == int(s.Repeat) {
			step = s
			processedMsg.RetryNum = 0
			retry = true
			break
		}
	}

	if retry {

		// Calculate the duration, set the timestamp and retryNum
		d, err := time.ParseDuration(step.BackoffDuration)
		if err != nil {
			c.logger.LogMessage.Msg = fmt.Sprintf("Error parsing backoff duration [%s]", step.BackoffDuration)
			c.logger.Log(err)
		}
		processedMsg.RetryStepIndex = int(step.Index)
		processedMsg.RetryNum = processedMsg.RetryNum + 1
		timestamp := time.Now().Add(d)
		processedMsg.RetryTimestamp = &timestamp

		c.logger.LogMessage.Msg = fmt.Sprintf("Attempting Retry with backoff duration [%s]", step.BackoffDuration)
		c.logger.Log(nil)

		switch strategy := *c.retryStrategy.Strategy; strategy {
		case openapi.RETRYSTRATEGIES_SIMPLE:
			// Sleep the thread for the duration
			time.Sleep(d)
			// Run again
			c.run(processedMsg, rawMessage, inputData)

		case openapi.RETRYSTRATEGIES_RETRY_TOPICS:

			retryTopicName := fmt.Sprintf("%s.%s", c.baseTopic, step.BackoffDuration)
			// Produce to the retry topic
			c.Producer.ProduceRetryMessage(processedMsg, rawMessage, retryTopicName)

		}
	} else {
		c.logger.LogMessage.Msg = "All retries attempted. Adding to Dead Letter Queue"
		c.logger.Log(nil)

		var dlqTopicName string
		if c.retryStrategy.DeadLetterSuffix != "" {
			dlqTopicName = fmt.Sprintf("%s.%s", c.baseTopic, c.retryStrategy.DeadLetterSuffix)
		} else {
			dlqTopicName = fmt.Sprintf("%s.dlq", c.baseTopic)
		}
		// Produce to the retry topic
		c.Producer.ProduceRetryMessage(processedMsg, rawMessage, dlqTopicName)
	}

}

func (c *Consumer) processMessage(msg *kafka.Message,
	input *openapi.AlgoInputModel) (processedMsg *types.ProcessedMsg) {

	processedMsg = &types.ProcessedMsg{}
	// Default to run - if header is set to false, then don't run
	processedMsg.Run = true
	// TraceID is the message key
	processedMsg.TraceID = string(msg.Key)

	// Parse the headers
	for _, header := range msg.Headers {
		switch header.Key {
		case "contentType":
			processedMsg.ContentType = string(header.Value)
		case "fileName":
			processedMsg.FileName = string(header.Value)
		case "messageDataType":
			processedMsg.MessageDataType.UnmarshalText(header.Value)
		case "endpointParams":
			processedMsg.EndpointParams = string(header.Value)
		case "retryStepIndex":
			retryStepIndex, _ := strconv.Atoi(string(header.Value))
			processedMsg.RetryStepIndex = retryStepIndex
		case "retryNum":
			retryNum, _ := strconv.Atoi(string(header.Value))
			processedMsg.RetryNum = retryNum
		case "retryTimestamp":
			i, _ := strconv.Atoi(string(header.Value))
			tm := time.Unix(int64(i), 0)
			processedMsg.RetryTimestamp = &tm
		case "run":
			b, _ := strconv.ParseBool(string(header.Value))
			processedMsg.Run = b
		}
	}

	if processedMsg.TraceID == "" {
		uuidTraceID, _ := uuid.NewV4()
		processedMsg.TraceID = strings.Replace(uuidTraceID.String(), "-", "", -1)
	}

	c.logger.LogMessage.TraceId = processedMsg.TraceID

	// If the content type is empty, use the first accepted content type
	if processedMsg.ContentType == "" {
		if len(input.ContentTypes) > 0 {
			processedMsg.ContentType = input.ContentTypes[0].Name
		}
	}

	// Check if the content is empty then this message is to trigger a run only
	if processedMsg.Run && len(msg.Value) < 1 {
		return
	}

	// TODO: Validate the content type

	// Save the data based on the delivery type
	processedMsg.InputData = types.InputData{}
	processedMsg.InputData.ContentType = processedMsg.ContentType

	// These input delivery types expect a byte stream
	if *input.InputDeliveryType == openapi.INPUTDELIVERYTYPES_STD_IN ||
		*input.InputDeliveryType == openapi.INPUTDELIVERYTYPES_HTTP ||
		*input.InputDeliveryType == openapi.INPUTDELIVERYTYPES_HTTPS {

		// If messageDataType is file reference then load file
		if processedMsg.MessageDataType == openapi.MESSAGEDATATYPES_FILE_REFERENCE {
			// Try to read the json
			var fileReference openapi.FileReference
			jsonErr := json.Unmarshal(msg.Value, &fileReference)

			if jsonErr != nil {
				c.logger.LogMessage.Msg = fmt.Sprintf("Failed to parse the FileReference json.")
				c.logger.Log(jsonErr)
			}

			// Read the file from storage
			// Initialize minio client object.
			minioClient, err := minio.New(c.storageConfig.Host,
				c.storageConfig.AccessKeyID,
				c.storageConfig.SecretAccessKey,
				c.storageConfig.UseSSL)
			if err != nil {
				c.logger.LogMessage.Msg = fmt.Sprintf("Failed to create minio client.")
				c.logger.Log(err)
			}
			object, err := minioClient.GetObject(fileReference.Bucket, fileReference.File, minio.GetObjectOptions{})
			if err != nil {
				c.logger.LogMessage.Msg = fmt.Sprintf("Failed to get file reference object from storage. [%v]", fileReference)
				c.logger.Log(err)
			}

			objectBytes, err := ioutil.ReadAll(object)
			if err != nil {
				c.logger.LogMessage.Msg = fmt.Sprintf("Failed to read file reference bytes from storage. [%v]", fileReference)
				c.logger.Log(err)
			}

			processedMsg.InputData.MsgSize = float64(binary.Size(msg.Value))
			processedMsg.InputData.DataSize = float64(binary.Size(objectBytes))

			processedMsg.InputData.IsFileReference = false
			processedMsg.InputData.Data = objectBytes

		} else {
			// If the data is embedded then copy the message value
			processedMsg.InputData.IsFileReference = false
			processedMsg.InputData.Data = msg.Value
		}

	} else {
		// These input delivery types expect a file

		// If messageDataType is file reference then ensure file exists and convert to container path
		if processedMsg.MessageDataType == openapi.MESSAGEDATATYPES_FILE_REFERENCE {

			// Write the file locally
			processedMsg.InputData.IsFileReference = true
			// Try to read the json
			var fileReference openapi.FileReference
			jsonErr := json.Unmarshal(msg.Value, &fileReference)

			if jsonErr != nil {
				c.logger.LogMessage.Msg = fmt.Sprintf("Failed to parse the FileReference json.")
				c.logger.Log(jsonErr)
			}

			// Read the file from storage
			// Initialize minio client object.
			minioClient, err := minio.New(c.storageConfig.Host,
				c.storageConfig.AccessKeyID,
				c.storageConfig.SecretAccessKey,
				c.storageConfig.UseSSL)
			if err != nil {
				c.logger.LogMessage.Msg = fmt.Sprintf("Failed to create minio client.")
				c.logger.Log(err)
			}
			object, err := minioClient.GetObject(fileReference.Bucket, fileReference.File, minio.GetObjectOptions{})
			if err != nil {
				c.logger.LogMessage.Msg = fmt.Sprintf("Failed to get file reference object from storage. [%v]", fileReference)
				c.logger.Log(err)
			}

			filePath := path.Join("/input", fileReference.File)
			localFile, err := os.Create(filePath)
			if err != nil {
				c.logger.LogMessage.Msg = fmt.Sprintf("Failed to create local file from reference object. [%v]", fileReference)
				c.logger.Log(err)
			}
			if _, err = io.Copy(localFile, object); err != nil {
				c.logger.LogMessage.Msg = fmt.Sprintf("Failed to copy byte stream from reference object to local file. [%v]", fileReference)
				c.logger.Log(err)
			}

			objStat, err := object.Stat()
			if err != nil {
				c.logger.LogMessage.Msg = fmt.Sprintf("Failed to Stat the file object to get file size. [%v]", fileReference)
				c.logger.Log(err)
			}

			processedMsg.InputData.MsgSize = float64(binary.Size(msg.Value))
			processedMsg.InputData.DataSize = float64(objStat.Size)

			processedMsg.InputData.FileReference = &fileReference

		} else {

			// The data is embedded so write the file locally as the algo expects a file
			processedMsg.InputData.IsFileReference = true
			if processedMsg.FileName == "" {
				processedMsg.FileName = processedMsg.TraceID
			}

			fullPathFile := path.Join("/input", processedMsg.FileName)

			fileReference := openapi.FileReference{
				File: processedMsg.FileName,
			}

			err := ioutil.WriteFile(fullPathFile, msg.Value, 0644)
			if err != nil {
				c.logger.LogMessage.Msg = fmt.Sprintf("Unable to write the embedded data to file [%s]", fullPathFile)
				c.logger.Log(err)
			}

			processedMsg.InputData.MsgSize = float64(binary.Size(msg.Value))
			processedMsg.InputData.DataSize = float64(binary.Size(msg.Value))

			processedMsg.InputData.FileReference = &fileReference

		}
	}

	return

}

func (c *Consumer) setInputDataMetrics(status string, processedMsg *types.ProcessedMsg) {

	c.metrics.MsgBytesInputCounter.WithLabelValues(c.metrics.DeploymentLabel,
		c.metrics.PipelineLabel,
		c.metrics.ComponentLabel,
		c.metrics.AlgoLabel,
		c.metrics.AlgoVersionLabel,
		c.metrics.AlgoIndexLabel,
		"",
		status).Add(float64(binary.Size(processedMsg.InputData.MsgSize)))

	c.metrics.DataBytesInputCounter.WithLabelValues(c.metrics.DeploymentLabel,
		c.metrics.PipelineLabel,
		c.metrics.ComponentLabel,
		c.metrics.AlgoLabel,
		c.metrics.AlgoVersionLabel,
		c.metrics.AlgoIndexLabel,
		"",
		status).Add(float64(binary.Size(processedMsg.InputData.DataSize)))

}
