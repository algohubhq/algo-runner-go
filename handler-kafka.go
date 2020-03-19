package main

import (
	"algo-runner-go/swagger"
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

type topicInputs map[string]*swagger.AlgoInputModel

func startConsumers() {

	// Create the base log message
	runnerLog := logMessage{
		Type_:   "Runner",
		Status:  "Started",
		Version: "1",
		Data: map[string]interface{}{
			"DeploymentOwnerUserName": config.DeploymentOwnerUserName,
			"DeploymentName":          config.DeploymentName,
			"AlgoOwnerUserName":       config.AlgoOwnerUserName,
			"AlgoName":                config.AlgoName,
			"AlgoVersionTag":          config.AlgoVersionTag,
			"AlgoInstanceName":        *instanceName,
		},
	}

	topicInputs := make(topicInputs)
	var topics []string
	algoName := fmt.Sprintf("%s/%s:%s[%d]", config.AlgoOwnerUserName, config.AlgoName, config.AlgoVersionTag, config.AlgoIndex)

	for _, pipe := range config.Pipes {

		if pipe.DestName == algoName {

			var input swagger.AlgoInputModel
			// Get the input associated with this route
			for i := range config.Inputs {
				if config.Inputs[i].Name == pipe.DestInputName {
					input = config.Inputs[i]
					break
				}
			}

			var topicConfig swagger.TopicConfigModel
			// Get the topic config associated with this route
			for x := range config.TopicConfigs {
				if config.TopicConfigs[x].SourceName == pipe.SourceName &&
					config.TopicConfigs[x].SourceOutputName == pipe.SourceOutputName {
					topicConfig = config.TopicConfigs[x]
					break
				}
			}

			// Replace the deployment username and name in the topic string
			topicName := strings.ToLower(strings.Replace(topicConfig.TopicName, "{deploymentownerusername}", config.DeploymentOwnerUserName, -1))
			topicName = strings.ToLower(strings.Replace(topicName, "{deploymentname}", config.DeploymentName, -1))

			topicInputs[topicName] = &input
			topics = append(topics, topicName)

			runnerLog.Msg = fmt.Sprintf("Listening to topic %s", topicName)
			runnerLog.log(nil)

		}

	}

	groupID := fmt.Sprintf("algorun-%s-%s-%s-%s-%d",
		config.DeploymentOwnerUserName,
		config.DeploymentName,
		config.AlgoOwnerUserName,
		config.AlgoName,
		config.AlgoIndex,
	)

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers":        *kafkaBrokers,
		"group.id":                 groupID,
		"client.id":                "algo-runner-go-client",
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
		"auto.offset.reset":        "earliest",
	}

	// Set the ssl config if enabled
	if CheckForKafkaTLS() {
		kafkaConfig["security.protocol"] = "ssl"
		kafkaConfig["ssl.ca.location"] = "/etc/ssl/certs/kafka-ca.crt"
		kafkaConfig["ssl.certificate.location"] = "/etc/ssl/certs/kafka-user.crt"
		kafkaConfig["ssl.key.location"] = "/etc/ssl/certs/kafka-user.key"
	}

	c, err := kafka.NewConsumer(&kafkaConfig)

	if err != nil {
		healthy = false
		runnerLog.Status = "Failed"
		runnerLog.Msg = fmt.Sprintf("Failed to create consumer.")
		runnerLog.log(err)

		os.Exit(1)
	}

	err = c.SubscribeTopics(topics, nil)

	waitForMessages(c, topicInputs)

}

func waitForMessages(c *kafka.Consumer, topicInputs topicInputs) {

	// Create the base log message
	runnerLog := logMessage{
		Type_:   "Runner",
		Status:  "Started",
		Version: "1",
		Data: map[string]interface{}{
			"DeploymentOwnerUserName": config.DeploymentOwnerUserName,
			"DeploymentName":          config.DeploymentName,
			"AlgoOwnerUserName":       config.AlgoOwnerUserName,
			"AlgoName":                config.AlgoName,
			"AlgoVersionTag":          config.AlgoVersionTag,
			"AlgoInstanceName":        *instanceName,
		},
	}

	defer c.Close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	data := make(map[string]map[*swagger.AlgoInputModel][]InputData)

	offsets := make(map[string]kafka.TopicPartition)

	waiting := true
	firstPoll := true

	for waiting == true {
		select {
		case sig := <-sigchan:

			runnerLog.Status = "Terminated"
			runnerLog.Msg = fmt.Sprintf("Caught signal %v: terminating the Kafka Consumer process.", sig)
			runnerLog.log(errors.New("Terminating"))

			healthy = false
			waiting = false

		default:

			var ev kafka.Event
			if firstPoll {
				ev = c.Poll(100)
			} else {
				ev = c.Poll(-1)
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
				runnerLog.log(nil)

				input := topicInputs[*e.TopicPartition.Topic]
				inputData, run, endpointParams := processMessage(e, input)

				if data[runID] == nil {
					data[runID] = make(map[*swagger.AlgoInputModel][]InputData)
				}

				data[runID][input] = append(data[runID][input], inputData)

				if run {

					// TODO: iterate over inputs to be sure at least one has data!
					// Can check to be sure all required inputs are fulfilled as well

					var runError error

					switch executor := strings.ToLower(config.Executor); executor {
					case "executable", "deletegated":
						runError = execRunner.run(runID, endpointParams, data[runID])
					case "http":
						runError = runHTTP(runID, endpointParams, data[runID])
					case "grpc":
						runError = errors.New("gRPC executor is not implemented")
					case "spark":
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

						offsets[runID] = offsetCommit

						_, offsetErr := c.StoreOffsets([]kafka.TopicPartition{offsets[runID]})
						if offsetErr != nil {
							runnerLog.Status = "Failed"
							runnerLog.Msg = fmt.Sprintf("Failed to store offsets for [%v]",
								[]kafka.TopicPartition{offsets[runID]})
							runnerLog.log(offsetErr)
						}

						_, commitErr := c.Commit()
						if commitErr != nil {
							runnerLog.Status = "Failed"
							runnerLog.Msg = fmt.Sprintf("Failed to commit offsets.")
							runnerLog.log(commitErr)
						}

						delete(data, runID)
						delete(offsets, runID)

					} else {
						runnerLog.Status = "Failed"
						runnerLog.Msg = "Failed to run algo"
						runnerLog.log(runError)
					}

					reqDuration := time.Since(startTime)
					runnerRuntimeHistogram.WithLabelValues(deploymentLabel, componentLabel, algoLabel, algoVersionLabel, algoIndexLabel).Observe(reqDuration.Seconds())

				} else {
					// Save the offset for the data that was only stored but not executed
					// Will be committed after successful run
					offsets[runID] = e.TopicPartition
				}

			case kafka.AssignedPartitions:
				healthy = true
				runnerLog.Msg = fmt.Sprintf("%v", e)
				runnerLog.log(nil)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				runnerLog.Msg = fmt.Sprintf("%v", e)
				runnerLog.log(nil)
				c.Unassign()
			case kafka.Error:
				runnerLog.Msg = fmt.Sprintf("Kafka Error: %v", e)
				runnerLog.log(nil)
				healthy = false
				waiting = false

			}
		}
	}

	healthy = false

}

func processMessage(msg *kafka.Message,
	input *swagger.AlgoInputModel) (inputData InputData, run bool, endpointParams string) {

	// Default to run - if header is set to false, then don't run
	run = true
	// runID is the message key
	runID = string(msg.Key)

	// Create the base log message
	runnerLog := logMessage{
		Type_:   "Runner",
		Status:  "Started",
		Version: "1",
		Data: map[string]interface{}{
			"DeploymentOwnerUserName": config.DeploymentOwnerUserName,
			"DeploymentName":          config.DeploymentName,
			"AlgoOwnerUserName":       config.AlgoOwnerUserName,
			"AlgoName":                config.AlgoName,
			"AlgoVersionTag":          config.AlgoVersionTag,
			"AlgoInstanceName":        *instanceName,
		},
	}

	// Parse the headers
	var contentType string
	var messageDataType string
	var fileName string
	for _, header := range msg.Headers {
		switch header.Key {
		case "contentType":
			contentType = string(header.Value)
		case "fileName":
			fileName = string(header.Value)
		case "messageDataType":
			messageDataType = string(header.Value)
		case "endpointParams":
			endpointParams = string(header.Value)
		case "run":
			b, _ := strconv.ParseBool(string(header.Value))
			run = b
		}
	}

	if runID == "" {
		uuidRunID, _ := uuid.NewV4()
		runID = strings.Replace(uuidRunID.String(), "-", "", -1)
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
	if input.InputDeliveryType == "StdIn" ||
		input.InputDeliveryType == "Http" ||
		input.InputDeliveryType == "Https" {

		// If messageDataType is file reference then load file
		if messageDataType == "FileReference" {
			// Try to read the json
			var fileReference swagger.FileReference
			jsonErr := json.Unmarshal(msg.Value, &fileReference)

			if jsonErr != nil {
				runnerLog.Status = "Failed"
				runnerLog.Msg = fmt.Sprintf("Failed to parse the FileReference json.")
				runnerLog.log(jsonErr)
			}

			// Read the file from storage
			// Initialize minio client object.
			minioClient, err := minio.New(storageConfig.host, storageConfig.accessKeyID, storageConfig.secretAccessKey, storageConfig.useSSL)
			if err != nil {
				runnerLog.Status = "Failed"
				runnerLog.Msg = fmt.Sprintf("Failed to create minio client.")
				runnerLog.log(err)
			}
			object, err := minioClient.GetObject(fileReference.Bucket, fileReference.File, minio.GetObjectOptions{})
			if err != nil {
				runnerLog.Status = "Failed"
				runnerLog.Msg = fmt.Sprintf("Failed to get file reference object from storage. [%v]", fileReference)
				runnerLog.log(err)
			}

			objectBytes, err := ioutil.ReadAll(object)
			if err != nil {
				runnerLog.Status = "Failed"
				runnerLog.Msg = fmt.Sprintf("Failed to read file reference bytes from storage. [%v]", fileReference)
				runnerLog.log(err)
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
		if messageDataType == "FileReference" {

			// Write the file locally
			inputData.isFileReference = true
			// Try to read the json
			var fileReference swagger.FileReference
			jsonErr := json.Unmarshal(msg.Value, &fileReference)

			if jsonErr != nil {
				runnerLog.Status = "Failed"
				runnerLog.Msg = fmt.Sprintf("Failed to parse the FileReference json.")
				runnerLog.log(jsonErr)
			}

			// Read the file from storage
			// Initialize minio client object.
			minioClient, err := minio.New(storageConfig.host, storageConfig.accessKeyID, storageConfig.secretAccessKey, storageConfig.useSSL)
			if err != nil {
				runnerLog.Status = "Failed"
				runnerLog.Msg = fmt.Sprintf("Failed to create minio client.")
				runnerLog.log(err)
			}
			object, err := minioClient.GetObject(fileReference.Bucket, fileReference.File, minio.GetObjectOptions{})
			if err != nil {
				runnerLog.Status = "Failed"
				runnerLog.Msg = fmt.Sprintf("Failed to get file reference object from storage. [%v]", fileReference)
				runnerLog.log(err)
			}

			filePath := path.Join("/input", fileReference.File)
			localFile, err := os.Create(filePath)
			if err != nil {
				runnerLog.Status = "Failed"
				runnerLog.Msg = fmt.Sprintf("Failed to create local file from reference object. [%v]", fileReference)
				runnerLog.log(err)
			}
			if _, err = io.Copy(localFile, object); err != nil {
				runnerLog.Status = "Failed"
				runnerLog.Msg = fmt.Sprintf("Failed to copy byte stream from reference object to local file. [%v]", fileReference)
				runnerLog.log(err)
			}

			inputData.fileReference = &fileReference

		} else {

			// The data is embedded so write the file locally as the algo expects a file
			inputData.isFileReference = true
			if fileName == "" {
				uuidRunID, _ := uuid.NewV4()
				fileName = strings.Replace(uuidRunID.String(), "-", "", -1)
			}

			fullPathFile := path.Join("/input", fileName)

			fileReference := swagger.FileReference{
				File: fileName,
			}

			err := ioutil.WriteFile(fullPathFile, msg.Value, 0644)
			if err != nil {
				runnerLog.Status = "Failed"
				runnerLog.Msg = fmt.Sprintf("Unable to write the embedded data to file [%s]", fullPathFile)
				runnerLog.log(err)
			}

			inputData.fileReference = &fileReference

		}
	}

	bytesProcessedCounter.WithLabelValues(deploymentLabel, componentLabel, algoLabel, algoVersionLabel, algoIndexLabel).Add(float64(binary.Size(msg.Value)))

	return

}

func produceOutputMessage(fileName string, topic string, data []byte) {

	// Create the base log message
	runnerLog := logMessage{
		Type_:   "Runner",
		Status:  "Started",
		Version: "1",
		Data: map[string]interface{}{
			"DeploymentOwnerUserName": config.DeploymentOwnerUserName,
			"DeploymentName":          config.DeploymentName,
			"AlgoOwnerUserName":       config.AlgoOwnerUserName,
			"AlgoName":                config.AlgoName,
			"AlgoVersionTag":          config.AlgoVersionTag,
			"AlgoInstanceName":        *instanceName,
		},
	}

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers": *kafkaBrokers,
	}

	// Set the ssl config if enabled
	if CheckForKafkaTLS() {
		kafkaConfig["security.protocol"] = "ssl"
		kafkaConfig["ssl.ca.location"] = "/etc/ssl/certs/kafka-ca.crt"
		kafkaConfig["ssl.certificate.location"] = "/etc/ssl/certs/kafka-user.crt"
		kafkaConfig["ssl.key.location"] = "/etc/ssl/certs/kafka-user.key"
	}

	p, err := kafka.NewProducer(&kafkaConfig)

	if err != nil {
		runnerLog.Status = "Failed"
		runnerLog.Type_ = "Local"
		runnerLog.Msg = "Failed to create Kafka message producer."
		runnerLog.log(err)

		return
	}

	doneChan := make(chan bool)

	go func() {
		defer close(doneChan)
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					runnerLog.Status = "Failed"
					runnerLog.Type_ = "Runner"
					runnerLog.Msg = fmt.Sprintf("Delivery failed for output: %v", m.TopicPartition.Topic)
					runnerLog.log(m.TopicPartition.Error)
				} else {
					runnerLog.Status = "Success"
					runnerLog.Type_ = "Runner"
					runnerLog.Msg = fmt.Sprintf("Delivered message to topic %s [%d] at offset %v",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					runnerLog.log(nil)
				}
				return
			case kafka.Error:
				runnerLog.Msg = fmt.Sprintf("Failed to deliver output message to Kafka: %v", e)
				runnerLog.log(nil)
				healthy = false

			default:

				runnerLog.Type_ = "Local"
				runnerLog.Msg = fmt.Sprintf("Ignored event: %s", ev)
				runnerLog.log(nil)
			}
		}
	}()

	// Create the headers
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "fileName", Value: []byte(fileName)})

	p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: []byte(runID), Value: data}

	// wait for delivery report goroutine to finish
	_ = <-doneChan

	p.Close()

}
