package main

import (
	"algo-runner-go/swagger"
	"bytes"
	"flag"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"github.com/radovskyb/watcher"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {

	configFilePtr := flag.String("config", "./config.json", "JSON config file to load")
	kafkaServersPtr := flag.String("kafka-servers", "localhost:9092", "Kafka broker addresses separated by a comma")

	flag.Parse()

	config := loadConfig(*configFilePtr)

	// Launch the server if not started
	if config.Serverless == false {

		var serverTerminated bool
		go func() {
			serverTerminated = startServer(config, *kafkaServersPtr)
			if serverTerminated {
				os.Exit(1)
			}
		}()

	}

	startConsumer(config, *kafkaServersPtr)

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
	kafkaServers string,
	runID string,
	inputMap map[*swagger.AlgoInputModel][]InputData) {

	// Create the base message
	algoLog := swagger.LogMessage{
		LogMessageType:        "Algo",
		EndpointOwnerUserName: config.EndpointOwnerUserName,
		EndpointUrlName:       config.EndpointUrlName,
		AlgoOwnerUserName:     config.AlgoOwnerUserName,
		AlgoUrlName:           config.AlgoUrlName,
		AlgoVersionTag:        config.AlgoVersionTag,
		Status:                "Started",
	}

	startTime := time.Now()

	command := getCommand(config)

	targetCmd := exec.Command(command[0], command[1:]...)

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// setup termination on kill signals
	go func() {
		sig := <-sigchan
		fmt.Printf("Caught signal %v. Killing server process: %s\n", sig, config.Entrypoint)
		if targetCmd != nil && targetCmd.Process != nil {
			val := targetCmd.Process.Kill()
			if val != nil {
				fmt.Printf("Killed algo process: %s - error %s\n", config.Entrypoint, val.Error())
			}
		}
	}()

	envs := getEnvironment(config)
	if len(envs) > 0 {
		//targetCmd.Env = envs
	}

	var stdout []byte
	var stderr []byte
	var cmdErr error

	var wg sync.WaitGroup

	// TODO: Write to the topic as error if no value
	if inputMap == nil {

		// 	return
	}

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

	for input, inputData := range inputMap {

		switch inputDeliveryType := input.InputDeliveryType; inputDeliveryType {
		case "StdIn":
			for _, data := range inputData {

				// get the writer for stdin
				writer, _ := targetCmd.StdinPipe()

				wg.Add(1)

				// Write to pipe in separate go-routine to prevent blocking
				go func(stdInData []byte) {
					defer wg.Done()

					writer.Write(stdInData)
					writer.Close()
				}(data.data)
			}

		case "Parameter":

			if input.Parameter != "" {
				targetCmd.Args = append(targetCmd.Args, input.Parameter)
			}
			for _, data := range inputData {
				targetCmd.Args = append(targetCmd.Args, string(data.data))
			}

		case "RepeatedParameter":

			for _, data := range inputData {
				if input.Parameter != "" {
					targetCmd.Args = append(targetCmd.Args, input.Parameter)
				}
				targetCmd.Args = append(targetCmd.Args, string(data.data))
			}

		case "DelimitedParameter":

			if input.Parameter != "" {
				targetCmd.Args = append(targetCmd.Args, input.Parameter)
			}
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

	wg.Add(1)

	// TODO: Setup all output file watchers
	w := watcher.New()
	w.SetMaxEvents(1)

	go func() {
		for {
			select {
			case event := <-w.Event:
				fmt.Println(event)

			case err := <-w.Error:
				fmt.Printf("Error watching output file/folder: %s/n", err)
			case <-w.Closed:
				return
			}
		}
	}()

	var sendStdOut bool

	for _, route := range config.PipelineRoutes {

		if route.SourceAlgoOwnerName == config.AlgoOwnerUserName &&
			route.SourceAlgoUrlName == config.AlgoUrlName {

			switch outputDeliveryType := route.SourceAlgoOutput.OutputDeliveryType; strings.ToLower(outputDeliveryType) {
			case "file":
				// Watch for a specific file.
				if err := w.AddRecursive(route.SourceAlgoOutput.OutputFilename); err != nil {
					// TODO: Log the error
				}
			case "folder":
				// Watch folder recursively for changes.
				if err := w.AddRecursive(route.SourceAlgoOutput.OutputPath); err != nil {
					// TODO: Log the error
				}
			case "stdout":
				sendStdOut = true
			}
		}

	}

	go func() {
		var b bytes.Buffer
		targetCmd.Stderr = &b

		defer wg.Done()

		stdout, cmdErr = targetCmd.Output()
		if b.Len() > 0 {
			stderr = b.Bytes()
		}
		b.Reset()
	}()

	wg.Wait()

	if timer != nil {
		timer.Stop()
	}

	if cmdErr != nil {

		outBytes := append(stderr, stdout...)
		algoLog.Status = "Failed"
		algoLog.Log = string(outBytes)

		produceLogMessage(getLogTopic(), kafkaServers, algoLog)

		return
	}

	execDuration := time.Since(startTime)

	if sendStdOut {
		stdoutTopic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.output.stdout",
			config.EndpointOwnerUserName,
			config.EndpointUrlName,
			config.AlgoOwnerUserName,
			config.AlgoUrlName))

		// Write to stdout output topic
		fileName, _ := uuid.NewV4()
		produceOutputMessage(runID, fileName.String(), stdoutTopic, kafkaServers, stdout)
	}

	// Write completion to log topic
	algoLog.Status = "Success"
	algoLog.RuntimeMs = int64(execDuration / time.Millisecond)
	algoLog.Log = string(stdout)

	produceLogMessage(getLogTopic(), kafkaServers, algoLog)

}

func runHTTP(config swagger.RunnerConfig,
	kafkaServers string,
	runID string,
	inputMap map[*swagger.AlgoInputModel][]InputData) {

	startTime := time.Now()

	// TODO: Write to the topic as error if no value
	if inputMap == nil {

		// 	return
	}

	var netClient = &http.Client{
		Timeout: time.Second * 10,
	}

	// Set the timeout
	if config.TimeoutSeconds > 0 {
		netClient.Timeout = time.Second * time.Duration(config.TimeoutSeconds)
	}

	topic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.output.default",
		config.EndpointOwnerUserName,
		config.EndpointUrlName,
		config.AlgoOwnerUserName,
		config.AlgoUrlName))

	for input, inputData := range inputMap {

		u, _ := url.Parse("localhost")

		u.Scheme = strings.ToLower(input.InputDeliveryType)
		if input.HttpPort > 0 {
			u.Host = fmt.Sprintf("localhost:%d", input.HttpPort)
		}
		u.Path = input.HttpPath

		q := u.Query()
		for _, param := range config.AlgoParams {
			q.Set(param.Name, param.Value)
		}
		u.RawQuery = q.Encode()

		for _, data := range inputData {
			request, reqErr := http.NewRequest(strings.ToLower(input.HttpVerb), u.String(), bytes.NewReader(data.data))
			if reqErr != nil {
				fmt.Fprintf(os.Stderr, "Error building request: %s\n", reqErr)
			}
			response, err := netClient.Do(request)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error getting response from http server: %s\n", err)
			} else {
				defer response.Body.Close()
				contents, err := ioutil.ReadAll(response.Body)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error reading response from http server: %s\n", err)
				}
				if response.StatusCode == 200 {
					fileName, _ := uuid.NewV4()
					produceOutputMessage(runID, fileName.String(), topic, kafkaServers, contents)
				} else {
					// TODO: produce the error to the log
				}

			}
		}

	}

	execDuration := time.Since(startTime).Seconds()

	// TODO: Write to output topic

	//if len(bytesWritten) > 0 {
	//		fmt.Printf("%s - Duration: %f seconds", bytesWritten, execDuration)
	//	} else {
	fmt.Printf("Duration: %f seconds", execDuration)
	//	}

}
