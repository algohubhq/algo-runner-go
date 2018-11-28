package main

import (
	"algo-runner-go/swagger"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"os/user"
	"path"
	"strconv"
	"strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/nu7hatch/gouuid"
)

type topicInputs map[string]*swagger.AlgoInputModel

func startConsumers() {

	// Create the base log message
	runnerLog := logMessage{
		LogMessageType: "Runner",
		Status:         "Started",
		RunnerLogData: &swagger.RunnerLogData{
			EndpointOwnerUserName: config.EndpointOwnerUserName,
			EndpointName:          config.EndpointName,
			AlgoOwnerUserName:     config.AlgoOwnerUserName,
			AlgoName:              config.AlgoName,
			AlgoVersionTag:        config.AlgoVersionTag,
			AlgoInstanceName:      *instanceName,
		},
	}

	topicInputs := make(topicInputs)
	var topics []string

	for _, pipe := range config.Pipes {

		if pipe.DestAlgoOwnerName == config.AlgoOwnerUserName &&
			pipe.DestAlgoName == config.AlgoName &&
			pipe.DestAlgoIndex == config.AlgoIndex {

			var input swagger.AlgoInputModel
			// Get the input associated with this route
			for i := range config.Inputs {
				if config.Inputs[i].Name == pipe.DestAlgoInputName {
					input = config.Inputs[i]
					break
				}
			}

			var topicConfig swagger.TopicConfigModel
			// Get the topic config associated with this route
			for x := range config.TopicConfigs {

				switch pipeType := pipe.PipeType; pipeType {
				case "Algo":

					if config.TopicConfigs[x].AlgoOwnerName == pipe.SourceAlgoOwnerName &&
						config.TopicConfigs[x].AlgoName == pipe.SourceAlgoName &&
						config.TopicConfigs[x].AlgoIndex == pipe.SourceAlgoIndex &&
						config.TopicConfigs[x].AlgoOutputName == pipe.SourceAlgoOutputName {
						topicConfig = config.TopicConfigs[x]
						break
					}

				case "DataSource":

					if config.TopicConfigs[x].PipelineDataSourceName == pipe.PipelineDataSourceName &&
						config.TopicConfigs[x].PipelineDataSourceIndex == pipe.PipelineDataSourceIndex {
						topicConfig = config.TopicConfigs[x]
						break
					}

				case "EndpointConnector":

					if config.TopicConfigs[x].EndpointConnectorOutputName == pipe.PipelineEndpointConnectorOutputName {
						topicConfig = config.TopicConfigs[x]
						break
					}

				}

			}

			// Replace the endpoint username and name in the topic string
			topicName := strings.ToLower(strings.Replace(topicConfig.TopicName, "{endpointownerusername}", config.EndpointOwnerUserName, -1))
			topicName = strings.ToLower(strings.Replace(topicName, "{endpointname}", config.EndpointName, -1))

			topicInputs[topicName] = &input
			topics = append(topics, topicName)

			runnerLog.RunnerLogData.Log = fmt.Sprintf("Listening to topic %s\n", topicName)
			runnerLog.log()

		}

	}

	groupID := fmt.Sprintf("%s-%s-%s-%s",
		config.EndpointOwnerUserName,
		config.EndpointName,
		config.AlgoOwnerUserName,
		config.AlgoName)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               *kafkaServers,
		"group.id":                        groupID,
		"client.id":                       "algo-runner-go-client",
		"enable.auto.commit":              false,
		"enable.auto.offset.store":        false,
		"auto.offset.reset":               "earliest",
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
	})

	if err != nil {
		healthy = false
		runnerLog.Status = "Failed"
		runnerLog.RunnerLogData.Log = fmt.Sprintf("Failed to create consumer. Fatal: %s\n", err)
		runnerLog.log()

		os.Exit(1)
	}

	err = c.SubscribeTopics(topics, nil)

	waitForMessages(c, topicInputs)

}

func waitForMessages(c *kafka.Consumer, topicInputs topicInputs) {

	// Create the base log message
	runnerLog := logMessage{
		LogMessageType: "Runner",
		Status:         "Started",
		RunnerLogData: &swagger.RunnerLogData{
			EndpointOwnerUserName: config.EndpointOwnerUserName,
			EndpointName:          config.EndpointName,
			AlgoOwnerUserName:     config.AlgoOwnerUserName,
			AlgoName:              config.AlgoName,
			AlgoVersionTag:        config.AlgoVersionTag,
			AlgoInstanceName:      *instanceName,
		},
	}

	defer c.Close()

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	data := make(map[string]map[*swagger.AlgoInputModel][]InputData)

	offsets := make(map[string]kafka.TopicPartition)

	waiting := true

	for waiting == true {
		select {
		case sig := <-sigchan:

			runnerLog.Status = "Terminated"
			runnerLog.RunnerLogData.Log = fmt.Sprintf("Caught signal %v: terminating the Kafka Consumer process.\n", sig)
			runnerLog.log()

			healthy = false
			waiting = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case *kafka.Message:

				runnerLog.RunnerLogData.Log = fmt.Sprintf("Kafka Message received on %s\n", e.TopicPartition)
				runnerLog.log()

				input := topicInputs[*e.TopicPartition.Topic]
				inputData, run := processMessage(e, input)

				if data[runID] == nil {
					data[runID] = make(map[*swagger.AlgoInputModel][]InputData)
				}

				data[runID][input] = append(data[runID][input], inputData)

				if run {

					// TODO: iterate over inputs to be sure at least one has data!
					// Can check to be sure all required inputs are fulfilled as well

					var runError error
					if strings.ToLower(config.ServerType) == "serverless" {
						runError = runExec(runID, data[runID])
					} else if strings.ToLower(config.ServerType) == "http" {
						runError = runHTTP(runID, data[runID])
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
							runnerLog.RunnerLogData.Log = fmt.Sprintf("Failed to store offsets for [%v] with error '%s'",
								[]kafka.TopicPartition{offsets[runID]},
								offsetErr)
							runnerLog.log()
						}

						_, commitErr := c.Commit()
						if commitErr != nil {
							runnerLog.Status = "Failed"
							runnerLog.RunnerLogData.Log = fmt.Sprintf("Failed to commit offsets with error '%s'",
								commitErr)
							runnerLog.log()
						}

						delete(data, runID)
						delete(offsets, runID)

					} else {
						runnerLog.Status = "Failed"
						runnerLog.RunnerLogData.Log = fmt.Sprintf("Failed to run Algo with error '%s'",
							runError)
						runnerLog.log()
					}

				} else {
					// Save the offset for the data that was only stored but not executed
					// Will be committed after successful run
					offsets[runID] = e.TopicPartition
				}

			case kafka.AssignedPartitions:
				runnerLog.RunnerLogData.Log = fmt.Sprintf("%v\n", e)
				runnerLog.log()
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				runnerLog.RunnerLogData.Log = fmt.Sprintf("%v\n", e)
				runnerLog.log()
				c.Unassign()
			case kafka.PartitionEOF:
				runnerLog.RunnerLogData.Log = fmt.Sprintf("Reached %v\n", e)
				runnerLog.log()
			case kafka.Error:
				runnerLog.RunnerLogData.Log = fmt.Sprintf("Kafka Error: %v\n", e)
				runnerLog.log()
				healthy = false
				waiting = false

			}
		}
	}

	healthy = false

}

func processMessage(msg *kafka.Message,
	input *swagger.AlgoInputModel) (inputData InputData, run bool) {

	// runID is the message key
	runID = string(msg.Key)

	// Create the base log message
	runnerLog := logMessage{
		LogMessageType: "Runner",
		Status:         "Started",
		RunnerLogData: &swagger.RunnerLogData{
			EndpointOwnerUserName: config.EndpointOwnerUserName,
			EndpointName:          config.EndpointName,
			AlgoOwnerUserName:     config.AlgoOwnerUserName,
			AlgoName:              config.AlgoName,
			AlgoVersionTag:        config.AlgoVersionTag,
			AlgoInstanceName:      *instanceName,
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
		case "run":
			b, _ := strconv.ParseBool(string(header.Value))
			run = b
		}
	}

	if runID == "" {
		uuidRunID, _ := uuid.NewV4()
		runID = strings.Replace(uuidRunID.String(), "-", "", -1)
	}

	// Check if the content is empty then this message is to trigger a run only
	if run && len(msg.Value) < 1 {
		return
	}

	// TODO: Validate the content type

	// Save the data based on the delivery type
	inputData = InputData{}
	inputData.contentType = contentType

	if input.InputDeliveryType == "StdIn" ||
		input.InputDeliveryType == "Http" ||
		input.InputDeliveryType == "Https" {

		// If messageDataType is file reference then load file
		if messageDataType == "FileReference" {
			// Try to read the json
			var fileReference FileReference
			jsonErr := json.Unmarshal(msg.Value, &fileReference)

			if jsonErr != nil {
				runnerLog.Status = "Failed"
				runnerLog.RunnerLogData.Log = fmt.Sprintf("Failed to parse the FileReference json with error: %v\n", jsonErr.Error())
				runnerLog.log()
			}

			// Read the file
			fullPathFile := path.Join(fileReference.FilePath, fileReference.FileName)
			fileBytes, err := ioutil.ReadFile(fullPathFile)
			if err != nil {
				runnerLog.Status = "Failed"
				runnerLog.RunnerLogData.Log = fmt.Sprintf("Failed to read the file reference with error: %v\n", err)
				runnerLog.log()
			}

			inputData.isFileReference = false
			inputData.data = fileBytes

		} else {
			// If the data is embedded then copy the message value
			inputData.isFileReference = false
			inputData.data = msg.Value
		}

	} else {

		// If messageDataType is file reference then ensure file exists and convert to container path
		if messageDataType == "FileReference" {

			// Try to read the json
			var fileReference FileReference
			jsonErr := json.Unmarshal(msg.Value, &fileReference)

			if jsonErr != nil {
				runnerLog.Status = "Failed"
				runnerLog.RunnerLogData.Log = fmt.Sprintf("Failed to parse the FileReference json with error: %v\n", jsonErr.Error())
				runnerLog.log()
			}
			// Check if the file exists
			fullPathFile := path.Join(fileReference.FilePath, fileReference.FileName)
			if _, err := os.Stat(fullPathFile); os.IsNotExist(err) {
				// Log error, File doesn't exist!
				runnerLog.Status = "Failed"
				runnerLog.RunnerLogData.Log = fmt.Sprintf("The file reference doesn't exist or is not accessible: [%s]\n", fullPathFile)
				runnerLog.log()
			}
			inputData.data = []byte(fullPathFile)

		} else {

			// The data is embedded so write the file locally as the algo expects a file
			inputData.isFileReference = true

			usr, _ := user.Current()
			dir := usr.HomeDir

			folder := path.Join(dir, "algorun", "data", runID, input.Name)
			if _, err := os.Stat(folder); os.IsNotExist(err) {
				os.MkdirAll(folder, os.ModePerm)
			}
			fullPathFile := path.Join(folder, fileName)
			err := ioutil.WriteFile(fullPathFile, msg.Value, 0644)
			if err != nil {
				runnerLog.Status = "Failed"
				runnerLog.RunnerLogData.Log = fmt.Sprintf("Unable to write the embedded data to file [%s] with error: %v\n", fullPathFile, err)
				runnerLog.log()
			}

			inputData.data = []byte(fullPathFile)
		}
	}

	return

}

func produceOutputMessage(fileName string, topic string, data []byte) {

	// Create the base log message
	runnerLog := logMessage{
		LogMessageType: "Runner",
		Status:         "Started",
		RunnerLogData: &swagger.RunnerLogData{
			EndpointOwnerUserName: config.EndpointOwnerUserName,
			EndpointName:          config.EndpointName,
			AlgoOwnerUserName:     config.AlgoOwnerUserName,
			AlgoName:              config.AlgoName,
			AlgoVersionTag:        config.AlgoVersionTag,
			AlgoInstanceName:      *instanceName,
		},
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": *kafkaServers,
	})

	if err != nil {
		runnerLog.Status = "Failed"
		runnerLog.LogMessageType = "Local"
		runnerLog.RunnerLogData.Log = fmt.Sprintf("Failed to create Kafka message producer: %s\n", err)
		runnerLog.log()

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
					runnerLog.LogMessageType = "Runner"
					runnerLog.RunnerLogData.Log = fmt.Sprintf("Delivery failed for output: %v\n", m.TopicPartition)
					runnerLog.log()
				} else {
					runnerLog.Status = "Success"
					runnerLog.LogMessageType = "Runner"
					runnerLog.RunnerLogData.Log = fmt.Sprintf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					runnerLog.log()
				}
				return

			default:

				runnerLog.LogMessageType = "Local"
				runnerLog.RunnerLogData.Log = fmt.Sprintf("Ignored event: %s\n", ev)
				runnerLog.log()
			}
		}
	}()

	// Create the headers
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "fileName", Value: []byte(fileName)})
	headers = append(headers, kafka.Header{Key: "run", Value: []byte("true")})

	p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: []byte(runID), Value: data}

	// wait for delivery report goroutine to finish
	_ = <-doneChan

	p.Close()

}

func produceLogMessage(logMessageBytes []byte) {

	// Create the base log message
	runnerLog := logMessage{
		LogMessageType: "Local",
		Status:         "Started",
		RunnerLogData: &swagger.RunnerLogData{
			EndpointOwnerUserName: config.EndpointOwnerUserName,
			EndpointName:          config.EndpointName,
			AlgoOwnerUserName:     config.AlgoOwnerUserName,
			AlgoName:              config.AlgoName,
			AlgoVersionTag:        config.AlgoVersionTag,
			AlgoInstanceName:      *instanceName,
		},
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": *kafkaServers,
		"default.topic.config": kafka.ConfigMap{
			"request.timeout.ms": 6000,
			"message.timeout.ms": 10000,
		},
		"message.send.max.retries": 1,
	})

	if err != nil {
		runnerLog.Status = "Failed"
		runnerLog.RunnerLogData.Log = fmt.Sprintf("Failed to create server message producer: %s\n", err)
		runnerLog.log()
		return
	}

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: logTopic, Partition: kafka.PartitionAny},
		Value:          logMessageBytes,
		Key:            []byte(runID),
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		healthy = false
		runnerLog.Status = "Failed"
		runnerLog.RunnerLogData.Log = fmt.Sprintf("Delivery of log message failed: %+v\n", m.TopicPartition)
		runnerLog.log()
	}

	close(deliveryChan)

}
