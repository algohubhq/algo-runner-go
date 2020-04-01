package runner

// We need this package to prevent cyclic dependencies

import (
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
)

// IRunner is an interface to define the functions of a runner
type IRunner interface {
	Run(string)
}

type Runner struct {
	Config  *openapi.AlgoRunnerConfig
	Logger  *logging.Logger
	Metrics *metrics.Metrics
}
