package main

import (
	"fmt"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// Global metrics variables
var (
	deploymentLabel  string
	algoLabel        string
	algoVersionLabel string
	algoIndexLabel   string

	runnerRuntimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "algorunner_run_duration_seconds",
		Help:    "The complete message processing duration in seconds",
		Buckets: []float64{0.005, 0.05, 0.25, 1, 2.5, 5, 7.5, 10, 20, 30},
	}, []string{"deployment", "algo", "version", "index"})

	algoRuntimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "algorunner_algo_duration_seconds",
		Help:    "The algo run duration in seconds",
		Buckets: []float64{0.005, 0.05, 0.25, 1, 2.5, 5, 7.5, 10, 20, 30},
	}, []string{"deployment", "algo", "version", "index"})

	bytesProcessedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_bytes_processed",
		Help: "The total number of bytes processed by the runner",
	}, []string{"deployment", "algo", "version", "index"})

	algoErrorCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_algo_error",
		Help: "The total number of errors from the algo",
	}, []string{"deployment", "algo", "version", "index"})

	runnerErrorCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_runner_error",
		Help: "The total number of errors from the runner",
	}, []string{"deployment", "algo", "version", "index"})
)

func registerMetrics() {

	deploymentLabel = fmt.Sprintf("%s/%s", config.DeploymentOwnerUserName, config.DeploymentName)
	algoLabel = fmt.Sprintf("%s/%s", config.AlgoOwnerUserName, config.AlgoName)
	algoVersionLabel = config.AlgoVersionTag
	algoIndexLabel = strconv.Itoa(int(config.AlgoIndex))

	prometheus.MustRegister(runnerRuntimeHistogram)
	prometheus.MustRegister(algoRuntimeHistogram)
	prometheus.MustRegister(bytesProcessedCounter)
	prometheus.MustRegister(algoErrorCounter)
	prometheus.MustRegister(runnerErrorCounter)

}
