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
	"time"
)

type topicInputs map[string]*swagger.AlgoInputModel

func startConsumers() {

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
				fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
				os.Exit(1)
			}

			fmt.Printf("Created Consumer %v\n", c)

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
			fmt.Printf("Caught signal %v: terminating\n", sig)
			waiting = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case *kafka.Message:

				fmt.Printf("%% Message received on %s\n",
					e.TopicPartition)

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

						fmt.Printf("%s", offsets[runID])
						_, offsetErr := c.StoreOffsets([]kafka.TopicPartition{offsets[runID]})
						if offsetErr != nil {
							// TODO: Log the error
						}

						_, commitErr := c.Commit()
						if commitErr != nil {
							// TODO: Log the error
						}

						delete(data, runID)
						delete(offsets, runID)

					} else {
						fmt.Fprintf(os.Stderr, "%s", runError)
					}

				} else {
					// Save the offset for the data that was only stored but not executed
					// Will be committed after successful run
					offsets[runID] = e.TopicPartition
				}

			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Unassign()
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				waiting = false
			}
		}
	}
}

func processMessage(msg *kafka.Message,
	input *swagger.AlgoInputModel) (inputData InputData, run bool) {

	// runID is the message key
	runID = string(msg.Key)

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
				fmt.Println(jsonErr.Error())
			}

			// Read the file
			fullPathFile := path.Join(fileReference.FilePath, fileReference.FileName)
			fileBytes, err := ioutil.ReadFile(fullPathFile)
			if err != nil {
				fmt.Println(err.Error())
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
				fmt.Println(jsonErr.Error())
			}
			// Check if the file exists
			fullPathFile := path.Join(fileReference.FilePath, fileReference.FileName)
			if _, err := os.Stat(fullPathFile); os.IsNotExist(err) {
				// TODO: Log error, File doesn't exist!
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
				// TODO: Log error
			}

			inputData.data = []byte(fullPathFile)
		}
	}

	return

}

func produceOutputMessage(runID string, fileName string, topic string, data []byte) {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaServers})

	if err != nil {
		fmt.Printf("Failed to create server message producer: %s\n", err)
		os.Exit(1)
	}

	doneChan := make(chan bool)

	go func() {
		defer close(doneChan)
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				return

			default:
				fmt.Printf("Ignored event: %s\n", ev)
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

func produceLogMessage(runID string, topic string, logMessage swagger.LogMessage) {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaServers})

	if err != nil {
		fmt.Printf("Failed to create server message producer: %s\n", err)
		os.Exit(1)
	}

	doneChan := make(chan bool)

	go func() {
		defer close(doneChan)
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				return

			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	logMessage.LogTimestamp = time.Now().UTC()

	logMessageBytes, err := json.Marshal(logMessage)

	p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: []byte(runID), Value: logMessageBytes}

	// wait for delivery report goroutine to finish
	_ = <-doneChan

	p.Close()

}
