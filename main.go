package main

import (
	"algo-runner-go/swagger"
	"bytes"
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {

	configFilePtr := flag.String("config", "./config.json", "JSON config file to load")
	kafkaServersPtr := flag.String("kafka-servers", "localhost:9092", "Kafka broker addresses separated by a comma")

	flag.Parse()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	config := loadConfig(*configFilePtr)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               *kafkaServersPtr,
		"group.id":                        "myGroup",
		"auto.offset.reset":               "earliest",
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	topicRoutes := make(map[string]swagger.PipelineRouteModel)

	var isServerStarted bool

	for _, route := range config.PipelineRoutes {

		// Launch the server if not started
		if (route.DestAlgoInput.InputDeliveryType == "Http" ||
			route.DestAlgoInput.InputDeliveryType == "Https") &&
			!isServerStarted {

			serverCmd := strings.Split(config.Entrypoint, " ")
			startServer(serverCmd)
			isServerStarted = true

		}

		if route.DestAlgoOwnerName == config.AlgoOwnerUserName &&
			route.DestAlgoUrlName == config.AlgoUrlName {

			switch routeType := route.RouteType; routeType {
			case "Algo":

				topic := fmt.Sprintf("algorun.%s.%s.algo.%s.%s",
					config.EndpointOwnerUserName,
					config.EndpointUrlName,
					route.SourceAlgoOwnerName,
					route.SourceAlgoUrlName)

				topicRoutes[topic] = route

				err = c.Subscribe(strings.ToLower(topic), nil)

			case "DataSource":

				topic := fmt.Sprintf("algorun.%s.%s.connector.%s",
					config.EndpointOwnerUserName,
					config.EndpointUrlName,
					route.PipelineDataSource.DataConnector.Name)

				topicRoutes[topic] = route

				err = c.Subscribe(strings.ToLower(topic), nil)

			case "EndpointSource":

				topic := fmt.Sprintf("algorun.%s.%s.output.%s",
					config.EndpointOwnerUserName,
					config.EndpointUrlName,
					route.PipelineEndpointSourceOutputName)

				topicRoutes[topic] = route

				err = c.Subscribe(strings.ToLower(topic), nil)

			}

		}

	}

	data := make(map[string]InputMap)

	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))

				route := topicRoutes[*e.TopicPartition.Topic]
				runID, inputData, run := processMessage(e, route, config)
				data[runID].inputs[route.DestAlgoInput] = append(data[runID].inputs[route.DestAlgoInput], inputData)

				if run {
					runExec(config, data[runID])
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
				run = false
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()

}

func processMessage(msg *kafka.Message,
	route swagger.PipelineRouteModel,
	config swagger.RunnerConfig) (runID string, inputData InputData, run bool) {

	// Parse the headers
	var fileName string
	for _, header := range msg.Headers {
		if header.Key == "runId" {
			runID = string(header.Value)
		}
		if header.Key == "fileName" {
			fileName = string(header.Value)
		}
		if header.Key == "run" {
			b, _ := strconv.ParseBool(string(header.Value))
			run = b
		}
	}

	// Save the data based on the delivery type
	inputData = InputData{}

	if route.DestAlgoInput.InputDeliveryType == "StdIn" ||
		route.DestAlgoInput.InputDeliveryType == "Http" ||
		route.DestAlgoInput.InputDeliveryType == "Https" {

		inputData.isFile = false
		inputData.data = msg.Value

	} else {

		inputData.isFile = true
		file := fmt.Sprintf("/data/%s", fileName)
		err := ioutil.WriteFile(file, msg.Value, 0644)
		if err != nil {
			// TODO: Log error
		}

		inputData.data = []byte(file)

	}

	return

}

func getCommand(config swagger.RunnerConfig) []string {

	cmd := strings.Split(config.Entrypoint, " ")

	for _, param := range config.AlgoParams {
		cmd = append(cmd, param.Name)
		if param.DataType.Name != "switch" {
			cmd = append(cmd, param.Value)
		}
	}

	return cmd
}

func getEnvironment(config swagger.RunnerConfig) []string {

	env := strings.Split(config.Entrypoint, " ")

	return env
}

func runExec(config swagger.RunnerConfig,
	inputMap InputMap) {

	startTime := time.Now()

	command := getCommand(config)

	targetCmd := exec.Command(command[0], command[1:]...)

	envs := getEnvironment(config)
	if len(envs) > 0 {
		//targetCmd.Env = envs
	}

	var out []byte
	var err error

	var wg sync.WaitGroup

	wgCount := 2

	// TODO: Write to the topic as error if no value
	// if inputMap == nil {

	// 	return
	// }

	wg.Add(wgCount)

	var timer *time.Timer

	// Set the timeout routine
	if config.TimeoutSeconds > 0 {
		timer = time.NewTimer(time.Duration(config.TimeoutSeconds) * time.Second)

		go func() {
			<-timer.C
			fmt.Printf("Killing process: %s\n", config.Entrypoint)
			if targetCmd != nil && targetCmd.Process != nil {
				val := targetCmd.Process.Kill()
				if val != nil {
					fmt.Printf("Killed process: %s - error %s\n", config.Entrypoint, val.Error())
				}
			}
		}()
	}

	for input, inputData := range inputMap.inputs {

		switch inputDeliveryType := input.InputDeliveryType; inputDeliveryType {
		case "StdIn":
			for _, data := range inputData {

				// get the writer for stdin
				writer, _ := targetCmd.StdinPipe()

				// Write to pipe in separate go-routine to prevent blocking
				go func() {
					defer wg.Done()

					writer.Write(data.data)
					writer.Close()
				}()
			}

		case "Parameter":

			targetCmd.Args = append(targetCmd.Args, input.Parameter)
			for _, data := range inputData {
				targetCmd.Args = append(targetCmd.Args, string(data.data))
			}

		case "RepeatedParameter":

			for _, data := range inputData {
				targetCmd.Args = append(targetCmd.Args, input.Parameter)
				targetCmd.Args = append(targetCmd.Args, string(data.data))
			}

		case "DelimitedParameter":

			targetCmd.Args = append(targetCmd.Args, input.Parameter)
			var buffer bytes.Buffer
			for i := 0; i < len(inputData); i++ {
				buffer.WriteString(string(inputData[i].data))
				if i != len(inputData)-1 {
					buffer.WriteString(input.ParameterDelimiter)
				}
			}
			targetCmd.Args = append(targetCmd.Args, buffer.String())
		}
	}

	go func() {
		var b bytes.Buffer
		targetCmd.Stderr = &b

		defer wg.Done()

		out, err = targetCmd.Output()
		if b.Len() > 0 {
			fmt.Printf("stderr: %s", b.Bytes())
		}
		b.Reset()
	}()

	wg.Wait()

	if timer != nil {
		timer.Stop()
	}

	if err != nil {

		fmt.Printf("Success=%t, Error=%s\n", targetCmd.ProcessState.Success(), err.Error())
		fmt.Printf("Out=%s\n", out)

		// TODO: Write error to output topic

		return
	}

	var bytesWritten string
	// if config.writeDebug == true {
	// 	os.Stdout.Write(out)
	// } else {
	bytesWritten = fmt.Sprintf("Wrote %d Bytes", len(out))
	//}

	execDuration := time.Since(startTime).Seconds()

	// TODO: Write to output topic

	if len(bytesWritten) > 0 {
		fmt.Printf("%s - Duration: %f seconds", bytesWritten, execDuration)
	} else {
		fmt.Printf("Duration: %f seconds", execDuration)
	}

}

func startServer(serverCmd []string) {

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd := exec.Command(serverCmd[0], serverCmd[1:]...)

	stdoutIn, _ := cmd.StdoutPipe()
	stderrIn, _ := cmd.StderrPipe()

	var errStdout, errStderr error
	stdout := io.MultiWriter(os.Stdout, &stdoutBuf)
	stderr := io.MultiWriter(os.Stderr, &stderrBuf)
	err := cmd.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Server cmd.Start() failed with '%s'\n", err)
	}

	go func() {
		_, errStdout = io.Copy(stdout, stdoutIn)
	}()

	go func() {
		_, errStderr = io.Copy(stderr, stderrIn)
	}()

	err = cmd.Wait()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Server cmd.Run() failed with %s\n", err)
	}
	if errStdout != nil || errStderr != nil {
		fmt.Fprintf(os.Stderr, "Server failed to capture stdout or stderr\n")
	}
	outStr, errStr := string(stdoutBuf.Bytes()), string(stderrBuf.Bytes())
	fmt.Printf("\nout:\n%s\nerr:\n%s\n", outStr, errStr)

}
