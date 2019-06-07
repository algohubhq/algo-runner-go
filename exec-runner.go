package main

import (
	"algo-runner-go/swagger"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	uuid "github.com/nu7hatch/gouuid"
)

func runExec(runID string,
	inputMap map[*swagger.AlgoInputModel][]InputData) (err error) {

	// Create the base message
	algoLog := logMessage{
		Type_:   "Algo",
		Status:  "Started",
		Version: "1",
		Data: map[string]interface{}{
			"RunId":                 runID,
			"EndpointOwnerUserName": config.EndpointOwnerUserName,
			"EndpointName":          config.EndpointName,
			"AlgoOwnerUserName":     config.AlgoOwnerUserName,
			"AlgoName":              config.AlgoName,
			"AlgoVersionTag":        config.AlgoVersionTag,
			"AlgoIndex":             config.AlgoIndex,
			"AlgoInstanceName":      *instanceName,
		},
	}

	startTime := time.Now()

	command := getCommand(config)

	targetCmd := exec.Command(command[0], command[1:]...)

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// setup termination on kill signals
	go func() {
		sig := <-sigchan

		algoLog.Msg = fmt.Sprintf("Caught signal %v. Killing algo process: %s", sig, config.Entrypoint)
		algoLog.log(nil)

		if targetCmd != nil && targetCmd.Process != nil {
			val := targetCmd.Process.Kill()
			if val != nil {
				algoLog.Status = "Terminated"
				algoLog.Msg = fmt.Sprintf("Killed algo process: %s", config.Entrypoint)
				algoLog.log(val)
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

	// Write to the topic as error if no value
	if inputMap == nil {
		algoLog.Status = "Failed"
		algoLog.Msg = "Attempted to run but input data is empty."
		algoLog.log(errors.New("Input data was empty"))

		return
	}

	var timer *time.Timer

	// Set the timeout routine
	if config.TimeoutSeconds > 0 {
		timer = time.NewTimer(time.Duration(config.TimeoutSeconds) * time.Second)

		go func() {
			<-timer.C

			algoLog.Status = "Timeout"
			algoLog.Msg = fmt.Sprintf("Algo timed out. Timeout value: %d seconds", config.TimeoutSeconds)
			algoLog.log(nil)

			if targetCmd != nil && targetCmd.Process != nil {
				val := targetCmd.Process.Kill()
				if val != nil {
					algoLog.Status = "Timeout"
					algoLog.Msg = fmt.Sprintf("Killed algo process due to timeout: %s", config.Entrypoint)
					algoLog.log(val)
				}
			}
		}()
	}

	// Write the stdin data or set the arguments for the input
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

	var sendStdOut bool

	outputWatcher := newOutputWatcher()

	algoName := fmt.Sprintf("%s/%s:%s[%d]", config.AlgoOwnerUserName, config.AlgoName, config.AlgoVersionTag, config.AlgoIndex)

	// Set the arguments for the output
	for _, output := range config.Outputs {

		handleOutput := config.WriteAllOutputs
		outputMessageDataType := "Embedded"

		// Check to see if there are any mapped routes for this output and get the message data type
		for i := range config.Pipes {
			if config.Pipes[i].SourceName == algoName {
				handleOutput = true
				outputMessageDataType = config.Pipes[i].SourceOutputMessageDataType
				break
			}
		}

		if handleOutput {

			switch outputDeliveryType := output.OutputDeliveryType; strings.ToLower(outputDeliveryType) {
			case "fileparameter":

				fileUUID, _ := uuid.NewV4()
				fileID := strings.Replace(fileUUID.String(), "-", "", -1)

				folder := path.Join("/data",
					config.EndpointOwnerUserName,
					config.EndpointName,
					runID,
					config.AlgoOwnerUserName,
					config.AlgoName,
					string(config.AlgoIndex),
					output.Name)

				fileFolder := path.Join(folder, fileID)
				if _, err := os.Stat(folder); os.IsNotExist(err) {
					os.MkdirAll(folder, os.ModePerm)
				}
				// Set the output parameter
				if output.Parameter != "" {
					targetCmd.Args = append(targetCmd.Args, output.Parameter)
					targetCmd.Args = append(targetCmd.Args, fileFolder)
				}

				// Watch for a specific file.
				outputWatcher.watch(fileFolder, config.AlgoIndex, &output, outputMessageDataType)

			case "folderparameter":
				// Watch folder for changes.

				folder := path.Join("/data",
					config.EndpointOwnerUserName,
					config.EndpointName,
					runID,
					config.AlgoOwnerUserName,
					config.AlgoName,
					string(config.AlgoIndex),
					output.Name)

				if _, err := os.Stat(folder); os.IsNotExist(err) {
					os.MkdirAll(folder, os.ModePerm)
				}
				// Set the output parameter
				if output.Parameter != "" {
					targetCmd.Args = append(targetCmd.Args, output.Parameter)
					targetCmd.Args = append(targetCmd.Args, folder)
				}

				// Watch for a specific file.
				outputWatcher.watch(folder, config.AlgoIndex, &output, outputMessageDataType)

			case "stdout":
				sendStdOut = true
			}

		}
	}

	outputWatcher.start()

	wg.Add(1)

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

		algoLog.Status = "Failed"
		// algoLog.AlgoLogData.RuntimeMs = int64(execDuration / time.Millisecond)
		algoLog.Msg = fmt.Sprintf("Stdout: %s | Stderr: %s", stdout, stderr)
		algoLog.log(cmdErr)

	} else {

		if sendStdOut {
			stdoutTopic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.%d.output.stdout",
				config.EndpointOwnerUserName,
				config.EndpointName,
				config.AlgoOwnerUserName,
				config.AlgoName,
				config.AlgoIndex))

			// Write to stdout output topic
			fileName, _ := uuid.NewV4()
			produceOutputMessage(fileName.String(), stdoutTopic, stdout)
		}

		// Write completion to log topic
		algoLog.Status = "Success"
		algoLog.Msg = fmt.Sprintf("Stdout: %s | Stderr: %s", stdout, stderr)
		algoLog.log(nil)

	}

	execDuration := time.Since(startTime)
	algoRuntimeHistogram.WithLabelValues(endpointLabel, algoLabel, algoLog.Status).Observe(execDuration.Seconds())

	outputWatcher.closeOutputWatcher()

	return nil

}

func getCommand(config swagger.AlgoRunnerConfig) []string {

	cmd := strings.Split(config.Entrypoint, " ")

	for _, param := range config.AlgoParams {
		cmd = append(cmd, param.Name)
		if param.DataType.Name != "switch" {
			cmd = append(cmd, param.Value)
		}
	}

	return cmd
}

func getEnvironment(config swagger.AlgoRunnerConfig) []string {

	env := strings.Split(config.Entrypoint, " ")

	return env
}
