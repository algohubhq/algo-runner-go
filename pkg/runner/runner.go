package runner

import (
	kafkaproducer "algo-runner-go/pkg/kafka/producer"
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	execrunner "algo-runner-go/pkg/runner/exec"
	httprunner "algo-runner-go/pkg/runner/http"
	"algo-runner-go/pkg/types"
)

type Runner struct {
	types.IRunner
	Config     *openapi.AlgoRunnerConfig
	ExecRunner *execrunner.ExecRunner
	HTTPRunner *httprunner.HTTPRunner
}

// NewRunner returns a new Runner struct
func NewRunner(config *openapi.AlgoRunnerConfig,
	producer *kafkaproducer.Producer,
	storageConfig *types.StorageConfig,
	instanceName string,
	kafkaBrokers string,
	logger *logging.Logger,
	metrics *metrics.Metrics) Runner {

	run := Runner{
		Config: config,
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

	// switch executor := c.Config.Executor; executor {
	// case openapi.EXECUTORS_EXECUTABLE, openapi.EXECUTORS_DELEGATED:
	// 	runError = execRunner.run(traceID, endpointParams, data[traceID])
	// case openapi.EXECUTORS_HTTP:
	// 	runError = runHTTP(traceID, endpointParams, data[traceID])
	// case openapi.EXECUTORS_GRPC:
	// 	runError = errors.New("gRPC executor is not implemented")
	// case openapi.EXECUTORS_SPARK:
	// 	runError = errors.New("Spark executor is not implemented")
	// default:
	// 	// Not implemented
	// 	runError = errors.New("Unknown executor is not supported")
	// }

	return run
}

func (r *Runner) Run(traceID string,
	endpointParams string,
	inputMap map[*openapi.AlgoInputModel][]types.InputData) (err error) {

	if r.Config.Executor == openapi.EXECUTORS_EXECUTABLE ||
		r.Config.Executor == openapi.EXECUTORS_DELEGATED {
		err = r.ExecRunner.Run(traceID,
			endpointParams,
			inputMap)
	}

	return err
}
