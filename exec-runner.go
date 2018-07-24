package main

import (
	"algo-runner-go/swagger"
	"bytes"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"github.com/radovskyb/watcher"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

func runExec(runID string,
	inputMap map[*swagger.AlgoInputModel][]InputData) (err error) {

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

		switch inputDeliveryType := strings.ToLower(input.InputDeliveryType); inputDeliveryType {
		case "stdin":

			// get the writer for stdin
			writer, _ := targetCmd.StdinPipe()
			for _, data := range inputData {
				// Write to stdin pipe
				writer.Write(data.data)
			}
			writer.Close()

		case "parameter":

			if input.Parameter != "" {
				targetCmd.Args = append(targetCmd.Args, input.Parameter)
			}
			for _, data := range inputData {
				targetCmd.Args = append(targetCmd.Args, string(data.data))
			}

		case "repeatedparameter":

			for _, data := range inputData {
				if input.Parameter != "" {
					targetCmd.Args = append(targetCmd.Args, input.Parameter)
				}
				targetCmd.Args = append(targetCmd.Args, string(data.data))
			}

		case "delimitedparameter":

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

	outputFiles := make(map[string]*swagger.AlgoOutputModel)

	go func() {
		for {
			select {
			case event := <-w.Event:
				// TODO: Test what happens when folder is monitored.
				// Determine how to get the output (remove the filename from the path?)
				algoOutput := outputFiles[event.Name()]

				fileOutputTopic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.output.%s",
					config.EndpointOwnerUserName,
					config.EndpointUrlName,
					config.AlgoOwnerUserName,
					config.AlgoUrlName,
					algoOutput.Name))

				// Write to stdout output topic
				fileName := event.Name()
				produceOutputMessage(runID, fileName, fileOutputTopic, stdout)

				fmt.Println(event)

			case err := <-w.Error:
				fmt.Printf("Error watching output file/folder: %s/n", err)
			case <-w.Closed:
				return
			}
		}
	}()

	var sendStdOut bool

	for _, output := range config.Outputs {

		writeOutput := false
		// Check to see if there are any mapped routes for this output
		if !config.WriteAllOutputs {
			for i := range config.PipelineRoutes {
				if config.PipelineRoutes[i].SourceAlgoOwnerName == config.AlgoOwnerUserName &&
					config.PipelineRoutes[i].SourceAlgoUrlName == config.AlgoUrlName {
					writeOutput = true
					break
				}
			}
		} else {
			writeOutput = true
		}

		if writeOutput {

			switch outputDeliveryType := output.OutputDeliveryType; strings.ToLower(outputDeliveryType) {
			case "file":
				// Watch for a specific file.
				if err := w.AddRecursive(output.OutputFilename); err != nil {
					// TODO: Log the error
				} else {
					outputFiles[output.OutputFilename] = &output
				}
			case "folder":
				// Watch folder recursively for changes.
				if err := w.AddRecursive(output.OutputPath); err != nil {
					// TODO: Log the error
				} else {
					outputFiles[output.OutputPath] = &output
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

		fmt.Printf("%s", targetCmd.Args)
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

		algoLog.Status = "Failed"
		algoLog.Log = fmt.Sprintf("%s\nStdout: %s\nStderr: %s", cmdErr, stdout, stderr)

		produceLogMessage(logTopic, algoLog)

		return cmdErr
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
		produceOutputMessage(runID, fileName.String(), stdoutTopic, stdout)
	}

	// Write completion to log topic
	algoLog.Status = "Success"
	algoLog.RuntimeMs = int64(execDuration / time.Millisecond)
	algoLog.Log = string(stdout)

	produceLogMessage(logTopic, algoLog)

	return nil

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
