package main

import (
	"algo-runner-go/swagger"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/nu7hatch/gouuid"
	"io/ioutil"
	"os"
	"os/signal"
	"os/user"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

type topicInputs map[string]*swagger.AlgoInputModel

func startConsumers() {

	// Create the base log message
	runnerLog := logMessage{
		LogMessageType:        "Runner",
		EndpointOwnerUserName: config.EndpointOwnerUserName,
		EndpointName:          config.EndpointName,
		AlgoOwnerUserName:     config.AlgoOwnerUserName,
		AlgoName:              config.AlgoName,
		AlgoVersionTag:        config.AlgoVersionTag,
		Status:                "Started",
	}

	var wg sync.WaitGroup

	for _, route := range config.PipelineRoutes {

		if route.DestAlgoOwnerName == config.AlgoOwnerUserName &&
			route.DestAlgoName == config.AlgoName {

			groupID := fmt.Sprintf("%s-%s-%s-%s",
				config.EndpointOwnerUserName,
				config.EndpointName,
				config.AlgoOwnerUserName,
				config.AlgoName)

			c, err := kafka.NewConsumer(&kafka.ConfigMap{
				"bootstrap.servers":               kafkaServers,
				"group.id":                        groupID,
				"client.id":                       "algo-runner-go-client",
				"enable.auto.commit":              false,
				"enable.auto.offset.store":        false,
				"auto.offset.reset":               "earliest",
				"go.events.channel.enable":        true,
				"go.application.rebalance.enable": true,
			})

			if err != nil {
				runnerLog.log("Failed", fmt.Sprintf("Failed to create consumer. Fatal: %s\n", err))
				os.Exit(1)
			}

			runnerLog.log(runnerLog.Status, fmt.Sprintf("Created Kafka Consumer %v\n", c))

			topicInputs := make(topicInputs)

			var input swagger.AlgoInputModel
			// Get the input associated with this route
			for i := range config.Inputs {
				if config.Inputs[i].Name == route.DestAlgoInputName {
					input = config.Inputs[i]
					break
				}
			}

			switch routeType := route.RouteType; routeType {
			case "Algo":

				topic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.%d.output.%s",
					config.EndpointOwnerUserName,
					config.EndpointName,
					route.SourceAlgoOwnerName,
					route.SourceAlgoName,
					route.SourceAlgoIndex,
					route.SourceAlgoOutputName))

				topicInputs[topic] = &input

				err = c.Subscribe(topic, nil)

			case "DataSource":

				topic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.connector.%s.%d",
					config.EndpointOwnerUserName,
					config.EndpointName,
					route.PipelineDataSourceName,
					route.PipelineDataSourceIndex))

				topicInputs[topic] = &input

				err = c.Subscribe(topic, nil)

			case "EndpointSource":

				topic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.output.%s",
					config.EndpointOwnerUserName,
					config.EndpointName,
					route.PipelineEndpointSourceOutputName))

				topicInputs[topic] = &input

				err = c.Subscribe(topic, nil)

			}

			go waitForMessages(&wg, c, topicInputs, route.DestAlgoIndex)

			wg.Add(1)

		}

	}

	wg.Wait()

}

func waitForMessages(wg *sync.WaitGroup, c *kafka.Consumer, topicInputs topicInputs, algoIndex int32) {

	// Create the base log message
	runnerLog := logMessage{
		LogMessageType:        "Runner",
		EndpointOwnerUserName: config.EndpointOwnerUserName,
		EndpointName:          config.EndpointName,
		AlgoOwnerUserName:     config.AlgoOwnerUserName,
		AlgoName:              config.AlgoName,
		AlgoVersionTag:        config.AlgoVersionTag,
		Status:                "Started",
	}

	defer c.Close()
	defer wg.Done()

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	data := make(map[string]map[*swagger.AlgoInputModel][]InputData)

	offsets := make(map[string]kafka.TopicPartition)

	waiting := true

	for waiting == true {
		select {
		case sig := <-sigchan:

			runnerLog.log("Terminated", fmt.Sprintf("Caught signal %v: terminating the Kafka Consumer process.\n", sig))
			waiting = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case *kafka.Message:

				runnerLog.log(runnerLog.Status, fmt.Sprintf("Kafka Message received on %s\n", e.TopicPartition))

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
						runError = runExec(runID, data[runID], algoIndex)
					} else if strings.ToLower(config.ServerType) == "http" {
						runError = runHTTP(runID, data[runID], algoIndex)
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
							runnerLog.log("Failed", fmt.Sprintf("Failed to store offsets for [%v] with error '%s'",
								[]kafka.TopicPartition{offsets[runID]},
								offsetErr))
						}

						_, commitErr := c.Commit()
						if commitErr != nil {
							runnerLog.log("Failed", fmt.Sprintf("Failed to commit offsets with error '%s'",
								commitErr))
						}

						delete(data, runID)
						delete(offsets, runID)

					} else {
						runnerLog.log("Failed", fmt.Sprintf("Failed to run Algo with error '%s'",
							runError))
					}

				} else {
					// Save the offset for the data that was only stored but not executed
					// Will be committed after successful run
					offsets[runID] = e.TopicPartition
				}

			case kafka.AssignedPartitions:
				runnerLog.log(runnerLog.Status, fmt.Sprintf("%v\n", e))
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				runnerLog.log(runnerLog.Status, fmt.Sprintf("%v\n", e))
				c.Unassign()
			case kafka.PartitionEOF:
				runnerLog.log(runnerLog.Status, fmt.Sprintf("Reached %v\n", e))
			case kafka.Error:
				runnerLog.log("Failed", fmt.Sprintf("Kafka Error: %v\n", e))
				waiting = false
			}
		}
	}
}

func processMessage(msg *kafka.Message,
	input *swagger.AlgoInputModel) (inputData InputData, run bool) {

	// runID is the message key
	runID = string(msg.Key)

	// Create the base log message
	runnerLog := logMessage{
		LogMessageType: "Runner",
		RunId:          runID,
		EndpointOwnerUserName: config.EndpointOwnerUserName,
		EndpointName:          config.EndpointName,
		AlgoOwnerUserName:     config.AlgoOwnerUserName,
		AlgoName:              config.AlgoName,
		AlgoVersionTag:        config.AlgoVersionTag,
		Status:                "Started",
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
				runnerLog.log("Failed", fmt.Sprintf("Failed to parse the FileReference json with error: %v\n", jsonErr.Error()))
			}

			// Read the file
			fullPathFile := path.Join(fileReference.FilePath, fileReference.FileName)
			fileBytes, err := ioutil.ReadFile(fullPathFile)
			if err != nil {
				runnerLog.log("Failed", fmt.Sprintf("Failed to read the file reference with error: %v\n", err))
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
				runnerLog.log("Failed", fmt.Sprintf("Failed to parse the FileReference json with error: %v\n", jsonErr.Error()))
			}
			// Check if the file exists
			fullPathFile := path.Join(fileReference.FilePath, fileReference.FileName)
			if _, err := os.Stat(fullPathFile); os.IsNotExist(err) {
				// Log error, File doesn't exist!
				runnerLog.log("Failed", fmt.Sprintf("The file reference doesn't exist or is not accessible: [%s]\n", fullPathFile))
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
				runnerLog.log("Failed", fmt.Sprintf("Unable to write the embedded data to file [%s] with error: %v\n", fullPathFile, err))
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
		RunId:          runID,
		EndpointOwnerUserName: config.EndpointOwnerUserName,
		EndpointName:          config.EndpointName,
		AlgoOwnerUserName:     config.AlgoOwnerUserName,
		AlgoName:              config.AlgoName,
		AlgoVersionTag:        config.AlgoVersionTag,
		Status:                "Started",
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaServers})

	if err != nil {
		runnerLog.LogMessageType = "Local"
		runnerLog.log("Failed", fmt.Sprintf("Failed to create Kafka message producer: %s\n", err))
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
					runnerLog.LogMessageType = "Runner"
					runnerLog.log("Failed", fmt.Sprintf("Delivery failed for output: %v\n", m.TopicPartition))
				} else {
					runnerLog.LogMessageType = "Runner"
					runnerLog.log("Success", fmt.Sprintf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset))
				}
				return

			default:
				runnerLog.LogMessageType = "Local"
				runnerLog.log(runnerLog.Status, fmt.Sprintf("Ignored event: %s\n", ev))
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

func produceLogMessage(topic string, lm *logMessage) {

	// Create the base log message
	runnerLog := logMessage{
		LogMessageType: "Local",
		RunId:          runID,
		EndpointOwnerUserName: config.EndpointOwnerUserName,
		EndpointName:          config.EndpointName,
		AlgoOwnerUserName:     config.AlgoOwnerUserName,
		AlgoName:              config.AlgoName,
		AlgoVersionTag:        config.AlgoVersionTag,
		Status:                "Started",
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaServers})

	if err != nil {
		runnerLog.log("Failed", fmt.Sprintf("Failed to create server message producer: %s\n", err))
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
					runnerLog.log("Failed", fmt.Sprintf("Delivery of log message failed: %v\n", m.TopicPartition))
				} else {
					runnerLog.log("Success", fmt.Sprintf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset))
				}
				return

			default:
				runnerLog.log(runnerLog.Status, fmt.Sprintf("Ignored event: %s\n", ev))
			}
		}
	}()

	logMessageBytes, err := json.Marshal(lm)

	p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: []byte(runID), Value: logMessageBytes}

	// wait for delivery report goroutine to finish
	_ = <-doneChan

	p.Close()

}
