package execrunner

import (
	kafkaproducer "algo-runner-go/pkg/kafka/producer"
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	ofw "algo-runner-go/pkg/outputfilewatcher"
	"algo-runner-go/pkg/storage"
	"algo-runner-go/pkg/types"
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
)

// ExecRunner holds the configuration for the external process and any file mirroring
type ExecRunner struct {
	types.IRunner
	Config         *openapi.AlgoRunnerConfig
	Logger         *logging.Logger
	Metrics        *metrics.Metrics
	Producer       *kafkaproducer.Producer
	StorageConfig  *storage.Storage
	InstanceName   string
	command        []string
	fileParameters map[string]string
	sendStdout     bool
}

// New creates a new ExecRunner.
func NewExecRunner(config *openapi.AlgoRunnerConfig,
	producer *kafkaproducer.Producer,
	storageConfig *storage.Storage,
	instanceName string,
	logger *logging.Logger,
	metrics *metrics.Metrics) *ExecRunner {

	command := getCommand(config)

	envs := getEnvironment(config)
	if len(envs) > 0 {
		//targetCmd.Env = envs
	}

	algoName := fmt.Sprintf("%s/%s:%s[%d]", config.Owner, config.Name, config.Version, config.Index)

	outputHandler := ofw.NewOutputFileWatcher(config, producer, storageConfig, metrics, instanceName, logger)
	var sendStdout bool
	fileParameters := make(map[string]string)

	// Set the arguments for the output
	for _, output := range config.Outputs {

		handleOutput := config.WriteAllOutputs
		outputMessageDataType := openapi.MESSAGEDATATYPES_EMBEDDED

		// Check to see if there are any mapped routes for this output and get the message data type
		for i := range config.Pipes {
			if config.Pipes[i].SourceName == algoName {
				handleOutput = true
				outputMessageDataType = *config.Pipes[i].SourceOutputMessageDataType
				break
			}
		}

		if handleOutput {

			if *output.OutputDeliveryType == openapi.OUTPUTDELIVERYTYPES_FILE_PARAMETER ||
				*output.OutputDeliveryType == openapi.OUTPUTDELIVERYTYPES_FOLDER_PARAMETER {

				// Watch folder for changes.

				folder := path.Join("/output")

				if _, err := os.Stat(folder); os.IsNotExist(err) {
					err = os.MkdirAll(folder, os.ModePerm)
					if err != nil {
						logger.Error(fmt.Sprintf("Failed to create output folder: %s\n", folder), err)
					}
				}

				if *output.OutputDeliveryType == openapi.OUTPUTDELIVERYTYPES_FILE_PARAMETER {
					// Set the output folder name parameter
					if output.Parameter != "" {
						fileParameters[output.Parameter] = folder
					}
				} else if *output.OutputDeliveryType == openapi.OUTPUTDELIVERYTYPES_FOLDER_PARAMETER {
					// Set the output folder name parameter
					if output.Parameter != "" {
						command = append(command, output.Parameter)
						command = append(command, folder)
					}
				}

				// Watch a folder folder
				// start a mc exec command
				go func() {
					outputHandler.Watch(folder, config.Index, &output, outputMessageDataType)
				}()

			} else if *output.OutputDeliveryType == openapi.OUTPUTDELIVERYTYPES_STD_OUT {
				sendStdout = true
			}

		}
	}

	return &ExecRunner{
		Config:         config,
		Producer:       producer,
		StorageConfig:  storageConfig,
		Logger:         logger,
		Metrics:        metrics,
		InstanceName:   instanceName,
		command:        command,
		fileParameters: fileParameters,
		sendStdout:     sendStdout,
	}

}

// Run starts the Executable
func (r *ExecRunner) Run(key string,
	traceID string,
	endpointParams string,
	inputMap map[*openapi.AlgoInputSpec][]types.InputData) (err error) {

	// Create the runner logger
	notifType := openapi.LOGTYPES_ALGO
	algoLogger := logging.NewLogger(
		&openapi.LogEntryModel{
			Type:    &notifType,
			Version: "1",
			Data: map[string]interface{}{
				"TraceId":          traceID,
				"DeploymentOwner":  r.Config.DeploymentOwner,
				"DeploymentName":   r.Config.DeploymentName,
				"AlgoOwner":        r.Config.Owner,
				"AlgoName":         r.Config.Name,
				"AlgoVersion":      r.Config.Version,
				"AlgoIndex":        r.Config.Index,
				"AlgoInstanceName": r.InstanceName,
			},
		},
		r.Metrics)

	targetCmd := r.newExecCmd()

	startTime := time.Now()

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// setup termination on kill signals
	go func() {
		sig := <-sigchan

		r.Logger.Error(fmt.Sprintf("Caught signal %v. Killing algo process: %s", sig, r.Config.Entrypoint), nil)

		if targetCmd != nil && targetCmd.Process != nil {
			err := targetCmd.Process.Kill()
			if err != nil {
				algoLogger.Error(fmt.Sprintf("Killed algo process: %s", r.Config.Entrypoint), err)
			}
		}
	}()

	// Write to the topic as error if no value
	if inputMap == nil {
		r.Logger.Error("Attempted to run but input data is empty.", errors.New("Input data was empty"))

		return
	}

	var timer *time.Timer

	// Set the timeout routine
	if r.Config.TimeoutSeconds > 0 {
		timer = time.NewTimer(time.Duration(r.Config.TimeoutSeconds) * time.Second)

		go func() {
			<-timer.C

			r.Logger.Error(fmt.Sprintf("Algo timed out. Timeout value: %d seconds", r.Config.TimeoutSeconds), nil)

			if targetCmd != nil && targetCmd.Process != nil {
				err := targetCmd.Process.Kill()
				if err != nil {
					r.Logger.Error(fmt.Sprintf("Killed algo process due to timeout: %s", r.Config.Entrypoint), err)
				}
			}
		}()
	}

	// Set the endpoint params as an environment variable
	targetCmd.Env = append(targetCmd.Env, fmt.Sprintf("ENDPOINT_PARAMS=%s", endpointParams))

	// Write the stdin data or set the arguments for the input
	for input, inputData := range inputMap {

		switch inputDeliveryType := *input.InputDeliveryType; inputDeliveryType {
		case openapi.INPUTDELIVERYTYPES_STD_IN:

			// get the writer for stdin
			writer, _ := targetCmd.StdinPipe()
			for _, data := range inputData {
				// Write to stdin pipe
				writer.Write(data.Data)
			}
			writer.Close()

		case openapi.INPUTDELIVERYTYPES_FILE_PARAMETER:

			if input.Parameter != "" {
				targetCmd.Args = append(targetCmd.Args, input.Parameter)
			}
			for _, data := range inputData {
				targetCmd.Args = append(targetCmd.Args, path.Join("/input", data.FileReference.File))
			}

		case "delimitedparameter":

			if input.Parameter != "" {
				targetCmd.Args = append(targetCmd.Args, input.Parameter)
			}
			var buffer bytes.Buffer
			for i := 0; i < len(inputData); i++ {
				buffer.WriteString(path.Join("/input", inputData[i].FileReference.File))
				if i != len(inputData)-1 {
					buffer.WriteString(input.ParameterDelimiter)
				}
			}
			targetCmd.Args = append(targetCmd.Args, buffer.String())
		}
	}

	// Iterate the fileParameters to append the traceid as the filename
	for parameter, folder := range r.fileParameters {
		fileFolder := path.Join(folder, traceID)

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
		r.Logger.Error("Erro occurred running Algo", cmdErr)
	}

	// Reminder: Successful outputs are handled by the output file watcher

	execDuration := time.Since(startTime)
	r.Metrics.AlgoRuntimeHistogram.WithLabelValues(r.Metrics.DeploymentLabel,
		r.Metrics.PipelineLabel,
		r.Metrics.ComponentLabel,
		r.Metrics.AlgoLabel,
		r.Metrics.AlgoVersionLabel,
		r.Metrics.AlgoIndexLabel).Observe(execDuration.Seconds())

	return nil

}

// New creates a new ExecCmd.
func (execRunner *ExecRunner) newExecCmd() *exec.Cmd {

	execCmd := exec.Command(execRunner.command[0], execRunner.command[1:]...)
	return execCmd

}

func getCommand(config *openapi.AlgoRunnerConfig) []string {

	cmd := strings.Split(config.Entrypoint, " ")

	for _, param := range config.Parameters {
		cmd = append(cmd, param.Name)
		if param.Value != "" {
			cmd = append(cmd, param.Value)
		}
	}

	return cmd
}

func getEnvironment(config *openapi.AlgoRunnerConfig) []string {

	env := strings.Split(config.Entrypoint, " ")

	return env
}
