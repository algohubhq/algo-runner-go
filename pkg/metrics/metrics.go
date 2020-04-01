package metrics

import (
	"algo-runner-go/pkg/openapi"
	"fmt"
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	RunnerRuntimeHistogram *prometheus.HistogramVec
	AlgoRuntimeHistogram   *prometheus.HistogramVec
	BytesInputCounter      *prometheus.CounterVec
	BytesOutputCounter     *prometheus.CounterVec
	AlgoErrorCounter       *prometheus.CounterVec
	RunnerErrorCounter     *prometheus.CounterVec
	DeploymentLabel        string
	PipelineLabel          string
	ComponentLabel         string
	AlgoLabel              string
	AlgoVersionLabel       string
	AlgoIndexLabel         string
}

// NewMetrics returns a new Metrics struct
func NewMetrics(config *openapi.AlgoRunnerConfig) Metrics {

	registerMetrics(config)

	return Metrics{
		RunnerRuntimeHistogram: runnerRuntimeHistogram,
		AlgoRuntimeHistogram:   algoRuntimeHistogram,
		BytesInputCounter:      bytesInputCounter,
		BytesOutputCounter:     bytesOutputCounter,
		AlgoErrorCounter:       algoErrorCounter,
		RunnerErrorCounter:     runnerErrorCounter,
		DeploymentLabel:        deploymentLabel,
		PipelineLabel:          pipelineLabel,
		ComponentLabel:         componentLabel,
		AlgoLabel:              algoLabel,
		AlgoVersionLabel:       algoVersionLabel,
		AlgoIndexLabel:         algoIndexLabel,
	}
}

var (
	deploymentLabel  string
	pipelineLabel    string
	componentLabel   string
	algoLabel        string
	algoVersionLabel string
	algoIndexLabel   string

	runnerRuntimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "algorunner_run_duration_seconds",
		Help:    "The complete message processing duration in seconds",
		Buckets: []float64{0.005, 0.05, 0.25, 1, 2.5, 5, 7.5, 10, 20, 30},
	}, []string{"deployment", "pipeline", "component", "name", "version", "index"})

	algoRuntimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "algorunner_algo_duration_seconds",
		Help:    "The algo run duration in seconds",
		Buckets: []float64{0.005, 0.05, 0.25, 1, 2.5, 5, 7.5, 10, 20, 30},
	}, []string{"deployment", "pipeline", "component", "name", "version", "index"})

	bytesInputCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_bytes_input_total",
		Help: "The total number of bytes input to the runner",
	}, []string{"deployment", "pipeline", "component", "name", "version", "index"})

	bytesOutputCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_bytes_output_total",
		Help: "The total number of bytes output from the runner",
	}, []string{"deployment", "pipeline", "component", "name", "version", "index"})

	algoErrorCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_algo_error",
		Help: "The total number of errors from the algo",
	}, []string{"deployment", "pipeline", "component", "name", "version", "index"})

	runnerErrorCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_runner_error",
		Help: "The total number of errors from the runner",
	}, []string{"deployment", "component", "name", "version", "index"})
)

func registerMetrics(config *openapi.AlgoRunnerConfig) {

	deploymentLabel = fmt.Sprintf("%s/%s", config.DeploymentOwnerUserName, config.DeploymentName)
	pipelineLabel = fmt.Sprintf("%s/%s", config.PipelineOwnerUserName, config.PipelineName)
	componentLabel = "algo"
	algoLabel = fmt.Sprintf("%s/%s", config.AlgoOwnerUserName, config.AlgoName)
	algoVersionLabel = config.AlgoVersionTag
	algoIndexLabel = strconv.Itoa(int(config.AlgoIndex))

	prometheus.MustRegister(runnerRuntimeHistogram)
	prometheus.MustRegister(algoRuntimeHistogram)
	prometheus.MustRegister(bytesInputCounter)
	prometheus.MustRegister(bytesOutputCounter)
	prometheus.MustRegister(algoErrorCounter)
	prometheus.MustRegister(runnerErrorCounter)

}

func (m *Metrics) CreateHTTPHandler() {

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if healthy {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(500)
		}

	})
	http.Handle("/metrics", promhttp.Handler())

	http.ListenAndServe(":10080", nil)

}
