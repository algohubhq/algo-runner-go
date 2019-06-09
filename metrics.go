package main

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Global metrics variables
var (
	endpointLabel string
	algoLabel     string

	runnerRuntimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "algorunner_run_duration_seconds",
		Help:    "The complete message processing duration in seconds",
		Buckets: []float64{0.005, 0.05, 0.25, 1},
	}, []string{"endpoint", "algo", "status"})

	algoRuntimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "algorunner_algo_duration_seconds",
		Help:    "The algo run duration in seconds",
		Buckets: []float64{0.005, 0.05, 0.25, 1, 2.5, 5, 7.5, 10, 20, 30},
	}, []string{"endpoint", "algo", "status"})

	bytesProcessedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_bytes_processed_total",
		Help: "The total number of bytes processed by the runner",
	}, []string{"endpoint", "algo", "status"})

	algoErrorCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_algo_error_total",
		Help: "The total number of errors from the algo",
	}, []string{"endpoint", "algo"})

	runnerErrorCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_runner_error_total",
		Help: "The total number of errors from the runner",
	}, []string{"endpoint", "algo"})
)

func registerMetrics() {

	endpointLabel = fmt.Sprintf("%s/%s", config.EndpointOwnerUserName, config.EndpointName)
	algoLabel = fmt.Sprintf("%s/%s", config.AlgoOwnerUserName, config.AlgoName)

	prometheus.MustRegister(runnerRuntimeHistogram)
	prometheus.MustRegister(algoRuntimeHistogram)
	prometheus.MustRegister(bytesProcessedCounter)
	prometheus.MustRegister(algoErrorCounter)
	prometheus.MustRegister(runnerErrorCounter)

}
