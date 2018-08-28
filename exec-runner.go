package main

import (
	"algo-runner-go/swagger"
	"bytes"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
)

func runExec(runID string,
	inputMap map[*swagger.AlgoInputModel][]InputData,
	algoIndex int32) (err error) {

	// Create the base message
	algoLog := swagger.LogMessage{
		LogMessageType: "Algo",
		RunId:          runID,
		EndpointOwnerUserName: config.EndpointOwnerUserName,
		EndpointName:          config.EndpointName,
		AlgoOwnerUserName:     config.AlgoOwnerUserName,
		AlgoName:              config.AlgoName,
		AlgoVersionTag:        config.AlgoVersionTag,
		AlgoIndex:             algoIndex,
		Status:                "Started",
	}

	startTime := time.Now().UTC()

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

	// Set the arguments for the output
	for _, output := range config.Outputs {

		handleOutput := config.WriteAllOutputs
		outputMessageDataType := "Embedded"

		// Check to see if there are any mapped routes for this output and get the message data type
		for i := range config.PipelineRoutes {
			if config.PipelineRoutes[i].SourceAlgoOwnerName == config.AlgoOwnerUserName &&
				config.PipelineRoutes[i].SourceAlgoName == config.AlgoName {
				handleOutput = true
				outputMessageDataType = config.PipelineRoutes[i].SourceAlgoOutputMessageDataType
				break
			}
		}

		if handleOutput {

			switch outputDeliveryType := output.OutputDeliveryType; strings.ToLower(outputDeliveryType) {
			case "fileparameter":

				fileUUID, _ := uuid.NewV4()
				fileID := strings.Replace(fileUUID.String(), "-", "", -1)

				usr, _ := user.Current()
				dir := usr.HomeDir

				folder := path.Join(dir, "algorun", "data", runID, output.Name)
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
				outputWatcher.watch(fileFolder, algoIndex, &output, outputMessageDataType)

			case "folderparameter":
				// Watch folder for changes.
				usr, _ := user.Current()
				dir := usr.HomeDir
				folder := path.Join(dir, "algorun", "data", runID, output.Name)
				if _, err := os.Stat(folder); os.IsNotExist(err) {
					os.MkdirAll(folder, os.ModePerm)
				}
				// Set the output parameter
				if output.Parameter != "" {
					targetCmd.Args = append(targetCmd.Args, output.Parameter)
					targetCmd.Args = append(targetCmd.Args, folder)
				}

				// Watch for a specific file.
				outputWatcher.watch(folder, algoIndex, &output, outputMessageDataType)

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

	execDuration := time.Since(startTime)

	if cmdErr != nil {

		algoLog.Status = "Failed"
		algoLog.LogSource = "stderr"
		algoLog.Log = fmt.Sprintf("%s\nStdout: %s\nStderr: %s", cmdErr, stdout, stderr)
		algoLog.RuntimeMs = int64(execDuration / time.Millisecond)

		produceLogMessage(runID, logTopic, algoLog)

		return cmdErr

	}

	if sendStdOut {
		stdoutTopic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.%d.output.stdout",
			config.EndpointOwnerUserName,
			config.EndpointName,
			config.AlgoOwnerUserName,
			config.AlgoName,
			algoIndex))

		// Write to stdout output topic
		fileName, _ := uuid.NewV4()
		produceOutputMessage(runID, fileName.String(), stdoutTopic, stdout)
	}

	// Write completion to log topic
	algoLog.Status = "Success"
	algoLog.LogSource = "stdout"
	algoLog.RuntimeMs = int64(execDuration / time.Millisecond)
	algoLog.Log = string(stdout)

	produceLogMessage(runID, logTopic, algoLog)

	outputWatcher.closeOutputWatcher()

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
