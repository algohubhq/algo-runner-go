package main

import (
	"algo-runner-go/swagger"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
)

// ExecOutputHandler handles all output files.
type ExecOutputHandler struct {
	outputs map[string]*output
}

type output struct {
	execCmd               *exec.Cmd
	outputMessageDataType string
	algoOutput            *swagger.AlgoOutputModel
	algoIndex             int32
}

type watchMessage struct {
	Status string `json:"status"`
	Event  struct {
		Time string `json:"time"`
		Size int64  `json:"size"`
		Path string `json:"path"`
		Type string `json:"type"`
	} `json:"events"`
	Source struct {
		Host      string `json:"host,omitempty"`
		Port      string `json:"port,omitempty"`
		UserAgent string `json:"userAgent,omitempty"`
	} `json:"source,omitempty"`
}

// mirrorMessage container for file mirror messages
type mirrorMessage struct {
	Status     string `json:"status"`
	Source     string `json:"source"`
	Target     string `json:"target"`
	Size       int64  `json:"size"`
	TotalCount int64  `json:"totalCount"`
	TotalSize  int64  `json:"totalSize"`
}

// causeMessage container for golang error messages
type causeMessage struct {
	Message string `json:"message"`
	Error   error  `json:"error"`
}

// errorMessage container for error messages
type errorMessage struct {
	Status string `json:"status"`
	Error  struct {
		Message   string            `json:"message"`
		Cause     causeMessage      `json:"cause"`
		Type      string            `json:"type"`
		CallTrace string            `json:"trace,omitempty"`
		SysInfo   map[string]string `json:"sysinfo"`
	} `json:"error,omitempty"`
}

// New creates a new Output Watcher.
func newOutputHandler() *ExecOutputHandler {

	return &ExecOutputHandler{
		outputs: make(map[string]*output),
	}

}

func (outputHandler *ExecOutputHandler) watch(fileFolder string, algoIndex int32, algoOutput *swagger.AlgoOutputModel, outputMessageDataType string) (err error) {

	execCmd := outputHandler.newCmd(fileFolder, outputMessageDataType)
	outputHandler.outputs[fileFolder] = &output{
		execCmd:               execCmd,
		algoOutput:            algoOutput,
		algoIndex:             algoIndex,
		outputMessageDataType: outputMessageDataType,
	}

	outputHandler.outputs[fileFolder].start()

	return nil

}

func (outputHandler *ExecOutputHandler) newCmd(src string, outputMessageDataType string) (execCmd *exec.Cmd) {

	// Create the base log message
	localLog := logMessage{
		Type_:   "Runner",
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

	mcPath := os.Getenv("MC_PATH")
	if mcPath == "" {
		mcPath = "mc"
	}

	cmd := exec.Command(mcPath)

	// If the output is embedded, run a watch, otherwise run a mirror
	if outputMessageDataType == "embedded" {
		cmd.Args = append(cmd.Args, "watch", "--json", "--quiet", src)
	} else {

		// The destination for the mc command uses an alias called "algorun" which is
		// mapped from an environment variable ex:
		// export MC_HOST_algorun=https://Q3AM3UQ867SPQQA43P2F:zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG@my.min.io
		// The bucket name is the deployment name

		// Ensure the envvar exists
		if s3Config.connectionString != "" {

			cmd.Args = append(cmd.Args, "mirror", "--json", "--quiet", "-w", src, destBucket)
		} else {
			localLog.Status = "Failed"
			localLog.Msg = "The s3 connection string is required for any file replication. Shutting down..."
			localLog.log(errors.New("S3 connection string missing"))

			os.Exit(1)
		}

	}

	return cmd

}

func (output *output) start() {

	// Create the base log message
	localLog := logMessage{
		Type_:   "Runner",
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

	// setup termination on kill signals
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigchan

		localLog.Status = "Terminated"
		localLog.Msg = fmt.Sprintf("Caught signal %v. Killing server process: mc\n", sig)
		localLog.log(nil)

		if output.execCmd != nil && output.execCmd.Process != nil {
			val := output.execCmd.Process.Kill()
			if val != nil {
				localLog.Status = "Terminated"
				localLog.Msg = fmt.Sprintf("Killed server process: mc - error %s\n", val.Error())
				localLog.log(nil)
			}
		}
	}()

	stdoutIn, _ := output.execCmd.StdoutPipe()
	stderrIn, _ := output.execCmd.StderrPipe()

	go func() {

		fileOutputTopic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.%d.output.%s",
			config.DeploymentOwnerUserName,
			config.DeploymentName,
			config.AlgoOwnerUserName,
			config.AlgoName,
			output.algoIndex,
			output.algoOutput.Name))

		// Scanner will split the lines (mc outputs jsonlines)
		scanner := bufio.NewScanner(stdoutIn)
		for scanner.Scan() {
			m := scanner.Bytes()
			if output.outputMessageDataType == "embedded" {
				// If embedded then the mc command is a watch
				var wm watchMessage
				jsonErr := json.Unmarshal(m, &wm)
				if jsonErr != nil || wm.Status == "error" || wm.Status == "" {
					if wm.Status != "" {
						var em errorMessage
						_ = json.Unmarshal(m, &em)
						localLog.Status = "Failed"
						localLog.Msg = fmt.Sprintf("mc watch command error. [%s]", em.Error)
						localLog.log(em.Error.Cause.Error)
					}
				}

				if wm.Status == "success" && wm.Event.Type == "ObjectCreated" {
					// Send contents of the file to kafka
					fileBytes, err := ioutil.ReadFile(wm.Event.Path)
					if err != nil {
						localLog.Status = "Failed"
						localLog.Msg = fmt.Sprintf("Output watcher unable to read the file [%s] from disk.", wm.Event.Path)
						localLog.log(err)
					}

					produceOutputMessage(wm.Event.Path, fileOutputTopic, fileBytes)
				}

			} else {

				// If not embedded then the mc command is a mirror
				var mm mirrorMessage
				jsonErr := json.Unmarshal(m, &mm)
				if jsonErr != nil || mm.Status == "error" || mm.Status == "" {

					if mm.Status != "" {
						var em errorMessage
						_ = json.Unmarshal(m, &em)
						localLog.Status = "Failed"
						localLog.Msg = fmt.Sprintf("mc mirror command error. [%s]", em.Error)
						localLog.log(em.Error.Cause.Error)
					}

				} else {
					// Send the file reference to Kafka
					// Try to create the json
					fileReference := swagger.FileReference{
						ServerAlias: s3Config.host,
						Bucket: mm.Target
						File: mm.Target,
					}
					jsonBytes, jsonErr := json.Marshal(fileReference)

					if jsonErr != nil {
						localLog.Status = "Failed"
						localLog.Msg = fmt.Sprintf("Unable to create the file reference json.")
						localLog.log(jsonErr)
					}

					produceOutputMessage(mm.Target, fileOutputTopic, jsonBytes)
				}

			}

		}
	}()

	go func() {
		// Scanner will split the lines (mc outputs jsonlines)
		scanner := bufio.NewScanner(stderrIn)
		for scanner.Scan() {
			m := scanner.Text()
			localLog.Status = "Failed"
			localLog.Msg = fmt.Sprintf("mc command stderr. [%s]", m)
			localLog.log(nil)
		}
	}()

	err := output.execCmd.Start()

	if err != nil {
		localLog.Status = "Failed"
		localLog.Msg = fmt.Sprintf("mc start failed with error '%s'\n", err)
		localLog.log(err)
	} else {
		localLog.Status = "Running"
		localLog.Msg = fmt.Sprintf("mc started with command '%s'\n", output.execCmd.String())
		localLog.log(nil)
	}

	errWait := output.execCmd.Wait()

	if errWait != nil {
		localLog.Status = "Failed"
		localLog.Msg = fmt.Sprintf("mc start failed with %s\n", errWait)
		localLog.log(errWait)
	}

	// If this is reached, the mc command has terminated (bad)

	localLog.Status = "Terminated"
	localLog.Msg = fmt.Sprintf("mc Terminated unexpectedly!")
	localLog.log(nil)

	os.Exit(1)

	return

}
