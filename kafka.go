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
	"strconv"
	"strings"
	"syscall"
)

type TopicInputs map[string]*swagger.AlgoInputModel

func startConsumer() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               kafkaServers,
		"group.id":                        "myGroup",
		"client.id":                       "algo-runner-go-client",
		"enable.auto.commit":              false,
		"enable.auto.offset.store":        false,
		"auto.offset.reset":               "latest",
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	topicInputs := make(TopicInputs)

	for _, route := range config.PipelineRoutes {

		if route.DestAlgoOwnerName == config.AlgoOwnerUserName &&
			route.DestAlgoUrlName == config.AlgoUrlName {

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

				topic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s",
					config.EndpointOwnerUserName,
					config.EndpointUrlName,
					route.SourceAlgoOwnerName,
					route.SourceAlgoUrlName))

				topicInputs[topic] = &input

				err = c.Subscribe(topic, nil)

			case "DataSource":

				topic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.connector.%s",
					config.EndpointOwnerUserName,
					config.EndpointUrlName,
					route.PipelineDataSourceName))

				topicInputs[topic] = &input

				err = c.Subscribe(topic, nil)

			case "EndpointSource":

				topic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.output.%s",
					config.EndpointOwnerUserName,
					config.EndpointUrlName,
					route.PipelineEndpointSourceOutputName))

				topicInputs[topic] = &input

				err = c.Subscribe(topic, nil)

			}

		}

	}

	waitForMessages(c, topicInputs)

	fmt.Printf("Closing consumer\n")
	c.Close()

}

func waitForMessages(c *kafka.Consumer, topicInputs TopicInputs) {

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
				runID, inputData, run := processMessage(e, input)

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
	input *swagger.AlgoInputModel) (runID string, inputData InputData, run bool) {

	// runID is the message key
	runID = string(msg.Key)

	// Parse the headers
	var contentType string
	var fileName string
	for _, header := range msg.Headers {
		switch header.Key {
		case "contentType":
			contentType = string(header.Value)
		case "fileName":
			fileName = string(header.Value)
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

		inputData.isFile = false
		inputData.data = msg.Value

	} else {

		inputData.isFile = true
		if _, err := os.Stat("/data"); os.IsNotExist(err) {
			os.MkdirAll("/data", os.ModePerm)
		}
		file := fmt.Sprintf("/data/%s", fileName)
		err := ioutil.WriteFile(file, msg.Value, 0644)
		if err != nil {
			// TODO: Log error
		}

		inputData.data = []byte(file)

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
	headers = append(headers, kafka.Header{Key: "runId", Value: []byte(runID)})
	headers = append(headers, kafka.Header{Key: "fileName", Value: []byte(fileName)})
	headers = append(headers, kafka.Header{Key: "run", Value: []byte("true")})

	p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value: data}

	// wait for delivery report goroutine to finish
	_ = <-doneChan

	p.Close()

}

func produceLogMessage(topic string, logMessage swagger.LogMessage) {

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

	logMessageBytes, err := json.Marshal(logMessage)

	p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value: logMessageBytes}

	// wait for delivery report goroutine to finish
	_ = <-doneChan

	p.Close()

}
