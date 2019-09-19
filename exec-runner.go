package main

import (
	"algo-runner-go/swagger"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
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

	execCmd := newExecCmd()
	targetCmd := execCmd.targetCmd

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

		if execCmd.sendStdout {
			stdoutTopic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.%d.output.stdout",
				config.EndpointOwnerUserName,
				config.EndpointName,
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
	algoRuntimeHistogram.WithLabelValues(endpointLabel, algoLabel, algoLog.Status).Observe(execDuration.Seconds())

	return nil

}
