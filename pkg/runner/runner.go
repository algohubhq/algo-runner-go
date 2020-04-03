package runner

import (
	kafkaproducer "algo-runner-go/pkg/kafka/producer"
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	execrunner "algo-runner-go/pkg/runner/exec"
	httprunner "algo-runner-go/pkg/runner/http"
	"algo-runner-go/pkg/storage"
	"algo-runner-go/pkg/types"
	"fmt"
)

type Runner struct {
	types.IRunner
	Config     *openapi.AlgoRunnerConfig
	Metrics    *metrics.Metrics
	ExecRunner *execrunner.ExecRunner
	HTTPRunner *httprunner.HTTPRunner
}

// NewRunner returns a new Runner struct
func NewRunner(config *openapi.AlgoRunnerConfig,
	producer *kafkaproducer.Producer,
	storageConfig *storage.Storage,
	instanceName string,
	kafkaBrokers string,
	logger *logging.Logger,
	metrics *metrics.Metrics) Runner {

	run := Runner{
		Config:  config,
		Metrics: metrics,
	}

	if config.Executor == openapi.EXECUTORS_EXECUTABLE ||
		config.Executor == openapi.EXECUTORS_DELEGATED {
		execRunner := execrunner.NewExecRunner(config, producer, storageConfig, instanceName, logger, metrics)
		run.ExecRunner = execRunner
	}

	if config.Executor == openapi.EXECUTORS_HTTP {
		httpRunner := httprunner.NewHTTPRunner(config, producer, storageConfig, instanceName, logger, metrics)
		run.HTTPRunner = httpRunner
	}

	return run
}

func (r *Runner) Run(traceID string,
	endpointParams string,
	inputMap map[*openapi.AlgoInputModel][]types.InputData) (err error) {

	switch executor := r.Config.Executor; executor {
	case openapi.EXECUTORS_EXECUTABLE, openapi.EXECUTORS_DELEGATED:
		err = r.ExecRunner.Run(traceID,
			endpointParams,
			inputMap)
	case openapi.EXECUTORS_HTTP:
		err = r.HTTPRunner.Run(traceID,
			endpointParams,
			inputMap)
	default:
		// Not implemented
		err = fmt.Errorf("Unsupported Executor type [%s] for this version of algo-runner", r.Config.Executor)
	}

	r.addInputBytesMetrics(err, inputMap)

	return err
}

func (r *Runner) addInputBytesMetrics(runError error,
	inputMap map[*openapi.AlgoInputModel][]types.InputData) {

	// Set the total input bytes processed
	status := "ok"
	if runError != nil {
		status = "failed"
	}

	for input, inputDataSlice := range inputMap {
		for _, inputData := range inputDataSlice {
			r.Metrics.MsgBytesInputCounter.WithLabelValues(r.Metrics.DeploymentLabel,
				r.Metrics.PipelineLabel,
				r.Metrics.ComponentLabel,
				r.Metrics.AlgoLabel,
				r.Metrics.AlgoVersionLabel,
				r.Metrics.AlgoIndexLabel,
				input.Name,
				status).Add(inputData.MsgSize)

			r.Metrics.DataBytesInputCounter.WithLabelValues(r.Metrics.DeploymentLabel,
				r.Metrics.PipelineLabel,
				r.Metrics.ComponentLabel,
				r.Metrics.AlgoLabel,
				r.Metrics.AlgoVersionLabel,
				r.Metrics.AlgoIndexLabel,
				input.Name,
				status).Add(inputData.DataSize)
		}
	}

}
