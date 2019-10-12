package main

import (
	"algo-runner-go/swagger"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	uuid "github.com/nu7hatch/gouuid"
)

// ExecRunner holds the configuration for the external process and any file mirroring
type ExecRunner struct {
	command        []string
	fileParameters map[string]string
	sendStdout     bool
}

// New creates a new ExecRunner.
func newExecRunner() *ExecRunner {

	// Create the base log message
	localLog := logMessage{
		Type_:   "Data",
		Status:  "Started",
		Version: "1",
		Data: map[string]interface{}{
			"DeploymentOwnerUserName": config.DeploymentOwnerUserName,
			"DeploymentName":          config.DeploymentName,
			"AlgoOwnerUserName":       config.AlgoOwnerUserName,
			"AlgoName":                config.AlgoName,
			"AlgoVersionTag":          config.AlgoVersionTag,
			"AlgoIndex":               config.AlgoIndex,
			"AlgoInstanceName":        *instanceName,
		},
	}

	command := getCommand(config)

	envs := getEnvironment(config)
	if len(envs) > 0 {
		//targetCmd.Env = envs
	}

	algoName := fmt.Sprintf("%s/%s:%s[%d]", config.AlgoOwnerUserName, config.AlgoName, config.AlgoVersionTag, config.AlgoIndex)

	outputHandler := newOutputHandler()
	var sendStdout bool
	fileParameters := make(map[string]string)

	// Set the arguments for the output
	for _, output := range config.Outputs {

		handleOutput := config.WriteAllOutputs
		outputMessageDataType := "embedded"

		// Check to see if there are any mapped routes for this output and get the message data type
		for i := range config.Pipes {
			if config.Pipes[i].SourceName == algoName {
				handleOutput = true
				outputMessageDataType = strings.ToLower(config.Pipes[i].SourceOutputMessageDataType)
				break
			}
		}

		if handleOutput {

			if strings.ToLower(output.OutputDeliveryType) == "fileparameter" ||
				strings.ToLower(output.OutputDeliveryType) == "folderparameter" {

				// Watch folder for changes.

				folder := path.Join("/output")

				if _, err := os.Stat(folder); os.IsNotExist(err) {
					err = os.MkdirAll(folder, os.ModePerm)
					if err != nil {
						localLog.Status = "Failed"
						localLog.Msg = fmt.Sprintf("Failed to create output folder: %s\n", folder)
						localLog.log(err)
					}
				}

				if strings.ToLower(output.OutputDeliveryType) == "fileparameter" {
					// Set the output folder name parameter
					if output.Parameter != "" {
						fileParameters[output.Parameter] = folder
					}
				} else if strings.ToLower(output.OutputDeliveryType) == "folderparameter" {
					// Set the output folder name parameter
					if output.Parameter != "" {
						command = append(command, output.Parameter)
						command = append(command, folder)
					}
				}

				// Watch a folder folder
				// start a mc exec command
				go func() {
					outputHandler.watch(folder, config.AlgoIndex, &output, outputMessageDataType)
				}()

			} else if strings.ToLower(output.OutputDeliveryType) == "stdout" {
				sendStdout = true
			}

		}
	}

	return &ExecRunner{
		command:        command,
		fileParameters: fileParameters,
		sendStdout:     sendStdout,
	}

}

// New creates a new ExecCmd.
func (execRunner *ExecRunner) newExecCmd() *exec.Cmd {

	execCmd := exec.Command(execRunner.command[0], execRunner.command[1:]...)
	return execCmd

}

func (execRunner *ExecRunner) run(runID string,
	inputMap map[*swagger.AlgoInputModel][]InputData) (err error) {

	// Create the base message
	algoLog := logMessage{
		Type_:   "Algo",
		Status:  "Started",
		Version: "1",
		Data: map[string]interface{}{
			"RunId":                   runID,
			"DeploymentOwnerUserName": config.DeploymentOwnerUserName,
			"DeploymentName":          config.DeploymentName,
			"AlgoOwnerUserName":       config.AlgoOwnerUserName,
			"AlgoName":                config.AlgoName,
			"AlgoVersionTag":          config.AlgoVersionTag,
			"AlgoIndex":               config.AlgoIndex,
			"AlgoInstanceName":        *instanceName,
		},
	}

	targetCmd := execRunner.newExecCmd()

	startTime := time.Now()

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
				targetCmd.Args = append(targetCmd.Args, path.Join("/input", data.fileReference.File))
			}

		case "repeatedparameter":

			for _, data := range inputData {
				if input.Parameter != "" {
					targetCmd.Args = append(targetCmd.Args, input.Parameter)
				}
				targetCmd.Args = append(targetCmd.Args, path.Join("/input", data.fileReference.File))
			}

		case "delimitedparameter":

			if input.Parameter != "" {
				targetCmd.Args = append(targetCmd.Args, input.Parameter)
			}
			var buffer bytes.Buffer
			for i := 0; i < len(inputData); i++ {
				buffer.WriteString(path.Join("/input", inputData[i].fileReference.File))
				if i != len(inputData)-1 {
					buffer.WriteString(input.ParameterDelimiter)
				}
			}
			targetCmd.Args = append(targetCmd.Args, buffer.String())
		}
	}

	// Iterate the fileParameters to append the runid as the filename
	for parameter, folder := range execRunner.fileParameters {
		fileUUID, _ := uuid.NewV4()
		fileID := strings.Replace(fileUUID.String(), "-", "", -1)
		fileFolder := path.Join(folder, fileID)

		targetCmd.Args = append(targetCmd.Args, parameter)
		targetCmd.Args = append(targetCmd.Args, fileFolder)
	}

	stdout, err := targetCmd.StdoutPipe()
	if err != nil {
		// log.Fatal(err)
	}
	stderr, err := targetCmd.StderrPipe()
	if err != nil {
		// log.Fatal(err)
	}

	stdoutBytes, _ := ioutil.ReadAll(stdout)
	fmt.Printf("%s\n", stdoutBytes)

	stderrBytes, _ := ioutil.ReadAll(stderr)
	fmt.Printf("%s\n", stderrBytes)

	if err := targetCmd.Start(); err != nil {
		// log.Fatal(err)
	}

	cmdErr := targetCmd.Wait()

	if timer != nil {
		timer.Stop()
	}

	if cmdErr != nil {

		algoLog.Status = "Failed"
		// algoLog.AlgoLogData.RuntimeMs = int64(execDuration / time.Millisecond)
		algoLog.Msg = fmt.Sprintf("Stdout: %s | Stderr: %s", stdout, stderr)
		algoLog.log(cmdErr)

	} else {

		if execRunner.sendStdout {
			stdoutTopic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.%d.output.stdout",
				config.DeploymentOwnerUserName,
				config.DeploymentName,
				config.AlgoOwnerUserName,
				config.AlgoName,
				config.AlgoIndex))

			// Write to stdout output topic
			fileName, _ := uuid.NewV4()
			produceOutputMessage(fileName.String(), stdoutTopic, stdoutBytes)
		}

		// Write completion to log topic
		algoLog.Status = "Success"
		algoLog.Msg = fmt.Sprintf("Stdout: %s | Stderr: %s", stdout, stderr)
		algoLog.log(nil)

	}

	execDuration := time.Since(startTime)
	algoRuntimeHistogram.WithLabelValues(deploymentLabel, algoLabel, algoLog.Status).Observe(execDuration.Seconds())

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
