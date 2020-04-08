package outputfilewatcher

import (
	kafkaproducer "algo-runner-go/pkg/kafka/producer"
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	"algo-runner-go/pkg/storage"
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

	uuid "github.com/nu7hatch/gouuid"
)

// New creates a new Output File Watcher.
func NewOutputFileWatcher(config *openapi.AlgoRunnerConfig,
	producer *kafkaproducer.Producer,
	storageConfig *storage.Storage,
	metrics *metrics.Metrics,
	instanceName string,
	logger *logging.Logger) *OutputFileWatcher {

	return &OutputFileWatcher{
		Outputs:       make(map[string]*Output),
		Config:        config,
		Producer:      producer,
		Metrics:       metrics,
		StorageConfig: storageConfig,
		InstanceName:  instanceName,
		Logger:        logger,
	}

}

// OutputFileWatcher handles all output files.
type OutputFileWatcher struct {
	Outputs       map[string]*Output
	Config        *openapi.AlgoRunnerConfig
	Producer      *kafkaproducer.Producer
	StorageConfig *storage.Storage
	Metrics       *metrics.Metrics
	InstanceName  string
	Logger        *logging.Logger
}

type Output struct {
	Logger                *logging.Logger
	Config                *openapi.AlgoRunnerConfig
	Producer              *kafkaproducer.Producer
	StorageConfig         *storage.Storage
	Metrics               *metrics.Metrics
	execCmd               *exec.Cmd
	outputMessageDataType openapi.MessageDataTypes
	algoOutput            *openapi.AlgoOutputModel
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

func (o *OutputFileWatcher) Watch(fileFolder string, algoIndex int32, algoOutput *openapi.AlgoOutputModel, outputMessageDataType openapi.MessageDataTypes) (err error) {

	execCmd := o.newCmd(fileFolder, outputMessageDataType)
	o.Outputs[fileFolder] = &Output{
		Logger:                o.Logger,
		Config:                o.Config,
		Producer:              o.Producer,
		StorageConfig:         o.StorageConfig,
		Metrics:               o.Metrics,
		execCmd:               execCmd,
		algoOutput:            algoOutput,
		algoIndex:             algoIndex,
		outputMessageDataType: outputMessageDataType,
	}

	o.Outputs[fileFolder].start()

	return nil

}

func (o *OutputFileWatcher) newCmd(src string, outputMessageDataType openapi.MessageDataTypes) (execCmd *exec.Cmd) {

	mcPath := os.Getenv("MC_PATH")
	if mcPath == "" {
		mcPath = "mc"
	}

	cmd := exec.Command(mcPath)

	// If the output is embedded, run a watch, otherwise run a mirror
	if outputMessageDataType == openapi.MESSAGEDATATYPES_EMBEDDED {
		cmd.Args = append(cmd.Args, "watch", "--json", "--quiet", src)
	} else {

		// The destination for the mc command uses an alias called "algorun" which is
		// mapped from an environment variable ex:
		// export MC_HOST_algorun=https://Q3AM3UQ867SPQQA43P2F:zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG@my.min.io
		// The bucket name is the deployment name

		// Ensure the envvar exists
		if o.StorageConfig.ConnectionString != "" {
			destBucket := strings.ToLower(fmt.Sprintf("algorun/%s.%s",
				o.Config.DeploymentOwnerUserName,
				o.Config.DeploymentName))
			cmd.Args = append(cmd.Args, "mirror", "--json", "--quiet", "-w", src, destBucket)
		} else {
			o.Logger.LogMessage.Msg = "The storage connection string is required for any file replication. Shutting down..."
			o.Logger.Log(errors.New("S3 connection string missing"))

			os.Exit(1)
		}

	}

	return cmd

}

func (output *Output) start() {

	// setup termination on kill signals
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigchan

		output.Logger.LogMessage.Msg = fmt.Sprintf("Caught signal %v. Killing mc process: mc\n", sig)
		output.Logger.Log(nil)

		if output.execCmd != nil && output.execCmd.Process != nil {
			val := output.execCmd.Process.Kill()
			if val != nil {
				output.Logger.LogMessage.Msg = fmt.Sprintf("Killed server process: mc - error %s\n", val.Error())
				output.Logger.Log(nil)
			}
		}
	}()

	stdoutIn, _ := output.execCmd.StdoutPipe()
	stderrIn, _ := output.execCmd.StderrPipe()

	go func() {

		fileOutputTopic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.%d.output.%s",
			output.Config.DeploymentOwnerUserName,
			output.Config.DeploymentName,
			output.Config.AlgoOwnerUserName,
			output.Config.AlgoName,
			output.algoIndex,
			output.algoOutput.Name))

		// Scanner will split the lines (mc outputs jsonlines)
		scanner := bufio.NewScanner(stdoutIn)
		for scanner.Scan() {
			m := scanner.Bytes()
			if output.outputMessageDataType == openapi.MESSAGEDATATYPES_EMBEDDED {
				// If embedded then the mc command is a watch
				var wm watchMessage
				jsonErr := json.Unmarshal(m, &wm)
				if jsonErr != nil || wm.Status == "error" || wm.Status == "" {
					if wm.Status != "" {
						var em errorMessage
						_ = json.Unmarshal(m, &em)
						output.Logger.LogMessage.Msg = fmt.Sprintf("mc watch command error. [%s]", em.Error)
						output.Logger.Log(em.Error.Cause.Error)
					}
				}

				if wm.Status == "success" && wm.Event.Type == "ObjectCreated" {
					// Send contents of the file to kafka
					fileBytes, err := ioutil.ReadFile(wm.Event.Path)
					if err != nil {
						output.Logger.LogMessage.Msg = fmt.Sprintf("Output watcher unable to read the file [%s] from disk.", wm.Event.Path)
						output.Logger.Log(err)
					}

					// TODO: Figure out how to get the traceID from the filename
					uuidTraceID, _ := uuid.NewV4()
					traceID := strings.Replace(uuidTraceID.String(), "-", "", -1)

					output.Metrics.DataBytesOutputCounter.WithLabelValues(output.Metrics.DeploymentLabel,
						output.Metrics.PipelineLabel,
						output.Metrics.ComponentLabel,
						output.Metrics.AlgoLabel,
						output.Metrics.AlgoVersionLabel,
						output.Metrics.AlgoIndexLabel,
						output.algoOutput.Name,
						"ok").Add(float64(wm.Event.Size))

					output.Producer.ProduceOutputMessage(traceID, wm.Event.Path, fileOutputTopic, output.algoOutput.Name, fileBytes)
				}

			} else {

				// If not embedded then the mc command is a mirror
				var mm mirrorMessage
				jsonErr := json.Unmarshal(m, &mm)
				if jsonErr != nil || mm.Status == "error" || mm.Status == "" {

					if mm.Status != "" {
						var em errorMessage
						_ = json.Unmarshal(m, &em)
						output.Logger.LogMessage.Msg = fmt.Sprintf("mc mirror command error. [%s]", em.Error)
						output.Logger.Log(em.Error.Cause.Error)
					}

				} else {
					// Send the file reference to Kafka
					// Try to create the json
					bucketName := fmt.Sprintf("%s.%s",
						strings.ToLower(output.Config.DeploymentOwnerUserName),
						strings.ToLower(output.Config.DeploymentName))
					fileReference := openapi.FileReference{
						Host:   output.StorageConfig.Host,
						Bucket: bucketName,
						File:   mm.Target,
					}
					jsonBytes, jsonErr := json.Marshal(fileReference)

					if jsonErr != nil {
						output.Logger.LogMessage.Msg = fmt.Sprintf("Unable to create the file reference json.")
						output.Logger.Log(jsonErr)
					}

					// TODO: Figure out how to get the traceID from the filename
					uuidTraceID, _ := uuid.NewV4()
					traceID := strings.Replace(uuidTraceID.String(), "-", "", -1)

					output.Metrics.DataBytesOutputCounter.WithLabelValues(output.Metrics.DeploymentLabel,
						output.Metrics.PipelineLabel,
						output.Metrics.ComponentLabel,
						output.Metrics.AlgoLabel,
						output.Metrics.AlgoVersionLabel,
						output.Metrics.AlgoIndexLabel,
						output.algoOutput.Name,
						"ok").Add(float64(mm.TotalSize))

					output.Producer.ProduceOutputMessage(traceID, mm.Target, fileOutputTopic, output.algoOutput.Name, jsonBytes)
				}

			}

		}
	}()

	go func() {
		// Scanner will split the lines (mc outputs jsonlines)
		scanner := bufio.NewScanner(stderrIn)
		for scanner.Scan() {
			m := scanner.Text()
			output.Logger.LogMessage.Msg = fmt.Sprintf("mc command stderr. [%s]", m)
			output.Logger.Log(nil)
		}
	}()

	err := output.execCmd.Start()

	if err != nil {
		output.Logger.LogMessage.Msg = fmt.Sprintf("mc start failed with error '%s'\n", err)
		output.Logger.Log(err)
	} else {
		output.Logger.LogMessage.Msg = fmt.Sprintf("mc started with command '%s'\n", output.execCmd.String())
		output.Logger.Log(nil)
	}

	errWait := output.execCmd.Wait()

	if errWait != nil {
		output.Logger.LogMessage.Msg = fmt.Sprintf("mc start failed with %s\n", errWait)
		output.Logger.Log(errWait)
	}

	// If this is reached, the mc command has terminated (bad)
	output.Logger.LogMessage.Msg = fmt.Sprintf("mc Terminated unexpectedly!")
	output.Logger.Log(nil)

	os.Exit(1)

	return

}
