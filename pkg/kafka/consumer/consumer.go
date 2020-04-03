package kafkaconsumer

import (
	k "algo-runner-go/pkg/kafka"
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
	HealthyChan   chan<- bool
	Config        *openapi.AlgoRunnerConfig
	Producer      *kafkaproducer.Producer
	StorageConfig *storage.Storage
	Logger        *logging.Logger
	Metrics       *metrics.Metrics
	InstanceName  string
	KafkaBrokers  string
	Runner        *runner.Runner
}

// NewConsumer returns a new Consumer struct
func NewConsumer(healthyChan chan<- bool,
	config *openapi.AlgoRunnerConfig,
	producer *kafkaproducer.Producer,
	storageConfig *storage.Storage,
	instanceName string,
	kafkaBrokers string,
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
		HealthyChan:   healthyChan,
		Config:        config,
		Producer:      producer,
		StorageConfig: storageConfig,
		Logger:        logger,
		Metrics:       metrics,
		InstanceName:  instanceName,
		KafkaBrokers:  kafkaBrokers,
		Runner:        &r,
	}
}

type topicInputs map[string]*openapi.AlgoInputModel

func (c *Consumer) StartConsumers() error {

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

			c.Logger.LogMessage.Msg = fmt.Sprintf("Listening to topic %s", topicName)
			c.Logger.Log(nil)

		}

	}

	groupID := fmt.Sprintf("algorun-%s-%s-%s-%s-%d-new",
		c.Config.DeploymentOwnerUserName,
		c.Config.DeploymentName,
		c.Config.AlgoOwnerUserName,
		c.Config.AlgoName,
		c.Config.AlgoIndex,
	)

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers":        c.KafkaBrokers,
		"group.id":                 groupID,
		"client.id":                "algo-runner-go-client",
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
		"auto.offset.reset":        "earliest",
	}

	// Set the ssl c.Config if enabled
	if k.CheckForKafkaTLS() {
		kafkaConfig["security.protocol"] = "ssl"
		kafkaConfig["ssl.ca.location"] = k.KafkaTLSCaLocation
		kafkaConfig["ssl.certificate.location"] = k.KafkaTLSUserLocation
		kafkaConfig["ssl.key.location"] = k.KafkaTLSKeyLocation
	}

	kc, err := kafka.NewConsumer(&kafkaConfig)

	if err != nil {
		c.HealthyChan <- false
		c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to create consumer.")
		c.Logger.Log(err)
		return err
	}

	err = kc.SubscribeTopics(topics, nil)
	if err != nil {
		c.HealthyChan <- false
		c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to subscribe to topics.")
		c.Logger.Log(err)
		return err
	}

	c.waitForMessages(kc, topicInputs)

	return nil

}

func (c *Consumer) waitForMessages(kc *kafka.Consumer, topicInputs topicInputs) {

	defer kc.Close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	data := make(map[string]map[*openapi.AlgoInputModel][]types.InputData)

	offsets := make(map[string]kafka.TopicPartition)

	waiting := true
	firstPoll := true

	for waiting == true {
		select {
		case sig := <-sigchan:

			c.Logger.LogMessage.Msg = fmt.Sprintf("Caught signal %v: terminating the Kafka Consumer process.", sig)
			c.Logger.Log(errors.New("Terminating"))

			c.HealthyChan <- false
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
					c.HealthyChan <- true
					firstPoll = false
				}
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:

				c.HealthyChan <- true
				c.Logger.LogMessage.TraceId = nil

				startTime := time.Now()

				c.Logger.LogMessage.Msg = fmt.Sprintf("Kafka Message received on %s", e.TopicPartition)
				c.Logger.Log(nil)

				input := topicInputs[*e.TopicPartition.Topic]
				traceID, inputData, run, endpointParams := c.processMessage(e, input)

				if data[traceID] == nil {
					data[traceID] = make(map[*openapi.AlgoInputModel][]types.InputData)
				}

				data[traceID][input] = append(data[traceID][input], inputData)

				if run {

					// TODO: iterate over inputs to be sure at least one has data!
					// Can check to be sure all required inputs are fulfilled as well

					runError := c.Runner.Run(traceID, endpointParams, data[traceID])

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
							c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to store offsets for [%v]",
								[]kafka.TopicPartition{offsets[traceID]})
							c.Logger.Log(offsetErr)
						}

						_, commitErr := kc.Commit()
						if commitErr != nil {
							c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to commit offsets.")
							c.Logger.Log(commitErr)
						}

						delete(data, traceID)
						delete(offsets, traceID)

					} else {
						c.Logger.LogMessage.Msg = "Failed to run algo"
						c.Logger.Log(runError)
					}

					reqDuration := time.Since(startTime)
					c.Metrics.RunnerRuntimeHistogram.WithLabelValues(c.Metrics.DeploymentLabel,
						c.Metrics.PipelineLabel,
						c.Metrics.ComponentLabel,
						c.Metrics.AlgoLabel,
						c.Metrics.AlgoVersionLabel,
						c.Metrics.AlgoIndexLabel).Observe(reqDuration.Seconds())

				} else {
					// Save the offset for the data that was only stored but not executed
					// Will be committed after successful run
					offsets[traceID] = e.TopicPartition
				}

			case kafka.AssignedPartitions:
				c.HealthyChan <- true
				c.Logger.LogMessage.Msg = fmt.Sprintf("%v", e)
				c.Logger.Log(nil)
				kc.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				c.Logger.LogMessage.Msg = fmt.Sprintf("%v", e)
				c.Logger.Log(nil)
				kc.Unassign()
			case kafka.Error:
				c.Logger.LogMessage.Msg = fmt.Sprintf("Kafka Error: %v", e)
				c.Logger.Log(nil)
				c.HealthyChan <- false
				waiting = false

			}
		}
	}

	c.HealthyChan <- false

}

func (c *Consumer) processMessage(msg *kafka.Message,
	input *openapi.AlgoInputModel) (traceID string, inputData types.InputData, run bool, endpointParams string) {

	// Default to run - if header is set to false, then don't run
	run = true
	// traceID is the message key
	traceID = string(msg.Key)

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

	c.Logger.LogMessage.TraceId = &traceID

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
	inputData = types.InputData{}
	inputData.ContentType = contentType

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
				c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to parse the FileReference json.")
				c.Logger.Log(jsonErr)
			}

			// Read the file from storage
			// Initialize minio client object.
			minioClient, err := minio.New(c.StorageConfig.Host,
				c.StorageConfig.AccessKeyID,
				c.StorageConfig.SecretAccessKey,
				c.StorageConfig.UseSSL)
			if err != nil {
				c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to create minio client.")
				c.Logger.Log(err)
			}
			object, err := minioClient.GetObject(fileReference.Bucket, fileReference.File, minio.GetObjectOptions{})
			if err != nil {
				c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to get file reference object from storage. [%v]", fileReference)
				c.Logger.Log(err)
			}

			objectBytes, err := ioutil.ReadAll(object)
			if err != nil {
				c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to read file reference bytes from storage. [%v]", fileReference)
				c.Logger.Log(err)
			}

			inputData.MsgSize = float64(binary.Size(msg.Value))
			inputData.DataSize = float64(binary.Size(objectBytes))

			inputData.IsFileReference = false
			inputData.Data = objectBytes

		} else {
			// If the data is embedded then copy the message value
			inputData.IsFileReference = false
			inputData.Data = msg.Value
		}

	} else {
		// These input delivery types expect a file

		// If messageDataType is file reference then ensure file exists and convert to container path
		if messageDataType == openapi.MESSAGEDATATYPES_FILE_REFERENCE {

			// Write the file locally
			inputData.IsFileReference = true
			// Try to read the json
			var fileReference openapi.FileReference
			jsonErr := json.Unmarshal(msg.Value, &fileReference)

			if jsonErr != nil {
				c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to parse the FileReference json.")
				c.Logger.Log(jsonErr)
			}

			// Read the file from storage
			// Initialize minio client object.
			minioClient, err := minio.New(c.StorageConfig.Host,
				c.StorageConfig.AccessKeyID,
				c.StorageConfig.SecretAccessKey,
				c.StorageConfig.UseSSL)
			if err != nil {
				c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to create minio client.")
				c.Logger.Log(err)
			}
			object, err := minioClient.GetObject(fileReference.Bucket, fileReference.File, minio.GetObjectOptions{})
			if err != nil {
				c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to get file reference object from storage. [%v]", fileReference)
				c.Logger.Log(err)
			}

			filePath := path.Join("/input", fileReference.File)
			localFile, err := os.Create(filePath)
			if err != nil {
				c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to create local file from reference object. [%v]", fileReference)
				c.Logger.Log(err)
			}
			if _, err = io.Copy(localFile, object); err != nil {
				c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to copy byte stream from reference object to local file. [%v]", fileReference)
				c.Logger.Log(err)
			}

			objStat, err := object.Stat()
			if err != nil {
				c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to Stat the file object to get file size. [%v]", fileReference)
				c.Logger.Log(err)
			}

			inputData.MsgSize = float64(binary.Size(msg.Value))
			inputData.DataSize = float64(objStat.Size)

			inputData.FileReference = &fileReference

		} else {

			// The data is embedded so write the file locally as the algo expects a file
			inputData.IsFileReference = true
			if fileName == "" {
				fileName = traceID
			}

			fullPathFile := path.Join("/input", fileName)

			fileReference := openapi.FileReference{
				File: fileName,
			}

			err := ioutil.WriteFile(fullPathFile, msg.Value, 0644)
			if err != nil {
				c.Logger.LogMessage.Msg = fmt.Sprintf("Unable to write the embedded data to file [%s]", fullPathFile)
				c.Logger.Log(err)
			}

			inputData.MsgSize = float64(binary.Size(msg.Value))
			inputData.DataSize = float64(binary.Size(msg.Value))

			inputData.FileReference = &fileReference

		}
	}

	return

}
