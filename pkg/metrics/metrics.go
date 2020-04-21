package metrics

import (
	"algo-runner-go/pkg/openapi"
	"fmt"
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fastjson"
)

type Metrics struct {
	HealthyChan               chan bool
	RunnerRuntimeHistogram    *prometheus.HistogramVec
	AlgoRuntimeHistogram      *prometheus.HistogramVec
	MsgBytesInputCounter      *prometheus.CounterVec
	MsgBytesOutputCounter     *prometheus.CounterVec
	DataBytesInputCounter     *prometheus.CounterVec
	DataBytesOutputCounter    *prometheus.CounterVec
	RetryCounter              *prometheus.CounterVec
	DlqCounter                *prometheus.CounterVec
	AlgoErrorCounter          *prometheus.CounterVec
	RunnerErrorCounter        *prometheus.CounterVec
	MsgOK                     *prometheus.CounterVec
	MsgNOK                    *prometheus.CounterVec
	MsgDropped                *prometheus.CounterVec
	ProducerQueueLen          *prometheus.Gauge
	EventIgnored              prometheus.Counter
	MsgInTransit              *prometheus.GaugeVec
	LibRdKafkaVersion         *prometheus.GaugeVec
	LastProducerStartTime     *prometheus.Gauge
	MetricCertExpirationTime  *prometheus.Gauge
	MetricCaExpirationTime    *prometheus.Gauge
	MetricKafkaEventsQueueLen *prometheus.Gauge
	MetricRDKafkaGlobal       *prometheus.GaugeVec
	MetricRDKafkaBroker       *prometheus.GaugeVec
	MetricRDKafkaTopic        *prometheus.GaugeVec
	MetricRDKafkaPartition    *prometheus.GaugeVec
	DeploymentLabel           string
	PipelineLabel             string
	ComponentLabel            string
	AlgoLabel                 string
	AlgoVersionLabel          string
	AlgoIndexLabel            string
}

// NewMetrics returns a new Metrics struct
func NewMetrics(healthyChan chan bool, config *openapi.AlgoRunnerConfig) Metrics {

	go func() {
		for h := range healthyChan {
			healthy = h
		}
	}()

	registerMetrics(config)

	return Metrics{
		RunnerRuntimeHistogram:    runnerRuntimeHistogram,
		AlgoRuntimeHistogram:      algoRuntimeHistogram,
		MsgBytesInputCounter:      msgBytesInputCounter,
		MsgBytesOutputCounter:     msgBytesOutputCounter,
		DataBytesInputCounter:     dataBytesInputCounter,
		DataBytesOutputCounter:    dataBytesOutputCounter,
		RetryCounter:              retryCounter,
		DlqCounter:                dlqCounter,
		AlgoErrorCounter:          algoErrorCounter,
		RunnerErrorCounter:        runnerErrorCounter,
		MsgOK:                     msgOK,
		MsgNOK:                    msgNOK,
		MsgDropped:                msgDropped,
		ProducerQueueLen:          &producerQueueLen,
		EventIgnored:              eventIgnored,
		MsgInTransit:              msgInTransit,
		LibRdKafkaVersion:         libRdKafkaVersion,
		LastProducerStartTime:     &lastProducerStartTime,
		MetricCertExpirationTime:  &metricCertExpirationTime,
		MetricCaExpirationTime:    &metricCaExpirationTime,
		MetricKafkaEventsQueueLen: &metricKafkaEventsQueueLen,
		MetricRDKafkaGlobal:       metricRDKafkaGlobal,
		MetricRDKafkaBroker:       metricRDKafkaBroker,
		MetricRDKafkaTopic:        metricRDKafkaTopic,
		MetricRDKafkaPartition:    metricRDKafkaPartition,

		DeploymentLabel:  deploymentLabel,
		PipelineLabel:    pipelineLabel,
		ComponentLabel:   componentLabel,
		AlgoLabel:        algoLabel,
		AlgoVersionLabel: algoVersionLabel,
		AlgoIndexLabel:   algoIndexLabel,
	}
}

var (
	healthy bool

	deploymentLabel  string
	pipelineLabel    string
	componentLabel   string
	algoLabel        string
	algoVersionLabel string
	algoIndexLabel   string

	rdHistoMetrics       = []string{"min", "max", "avg", "p50", "p95", "p99"}
	rdGlobalMetrics      = []string{"replyq", "msg_cnt", "msg_size", "tx", "tx_bytes", "rx", "rx_bytes", "txmsgs", "txmsgs_bytes", "rxmsgs", "rxmsgs_bytes"}
	rdBrokerMetrics      = []string{"outbuf_cnt", "outbuf_msg_cnt", "waitresp_cnt", "waitresp_msg_cnt", "tx", "txbytes", "req_timeouts", "rx", "rxbytes", "connects", "disconnects"}
	rdBrokerHistoMetrics = []string{"int_latency", "outbuf_latency", "rtt"}
	rdTopicMetrics       = []string{"batchsize", "batchcnt"}
	rdPartitionMetrics   = []string{"msgq_cnt", "msgq_bytes", "xmit_msgq_cnt", "msgs_inflight"}

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

	msgBytesInputCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_msg_bytes_input_total",
		Help: "The total number of bytes input to the runner from messages. (Kafka)",
	}, []string{"deployment", "pipeline", "component", "name", "version", "index", "input", "status"})

	msgBytesOutputCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_msg_bytes_output_total",
		Help: "The total number of bytes output from the runner from messages. (Kafka)",
	}, []string{"deployment", "pipeline", "component", "name", "version", "index", "output", "status"})

	dataBytesInputCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_data_bytes_input_total",
		Help: "The total number of bytes input to the runner including file data",
	}, []string{"deployment", "pipeline", "component", "name", "version", "index", "input", "status"})

	dataBytesOutputCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_data_bytes_output_total",
		Help: "The total number of bytes output from the runner including file data",
	}, []string{"deployment", "pipeline", "component", "name", "version", "index", "output", "status"})

	retryCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_retry_total",
		Help: "The total number of execution retries",
	}, []string{"deployment", "pipeline", "component", "name", "version", "index", "step", "repeat"})

	dlqCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_retry_total",
		Help: "The total number of message",
	}, []string{"deployment", "pipeline", "component", "name", "version", "index"})

	algoErrorCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_algo_error",
		Help: "The total number of errors from the algo",
	}, []string{"deployment", "pipeline", "component", "name", "version", "index"})

	runnerErrorCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "algorunner_runner_error",
		Help: "The total number of errors from the runner",
	}, []string{"deployment", "component", "name", "version", "index"})

	msgSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_client_requests_cnt",
			Help: "Number of kafka requests sent",
		},
		[]string{"topic"},
	)
	msgOK = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_client_ack_cnt",
			Help: "Number of kafka ACKed requests received",
		},
		[]string{"topic"},
	)
	msgNOK = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_client_err_cnt",
			Help: "Number of kafka Errored requests",
		},
		[]string{"topic", "error"},
	)
	msgInTransit = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_client_messages_in_transit",
			Help: "Number of kafka messages in transit",
		},
		[]string{"producer_id"},
	)
	msgDropped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_client_dropped_cnt",
			Help: "Number of kafka Errored requests which are dropped",
		},
		[]string{"topic", "error"},
	)
	producerQueueLen = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "producer_kafka_queue_len",
			Help: "Number of messages and requests waiting to be transmitted to the broker as well as delivery reports queued for the application",
		},
	)
	eventIgnored = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_client_events_ignored_cnt",
			Help: "Number of kafka events which are ignored",
		},
	)

	libRdKafkaVersion = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "producer_kafka_librdkafka_version",
			Help: "Version of underlying librdkafka library",
		},
		[]string{"version"},
	)

	lastProducerStartTime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_last_producer_start_time",
			Help: "Time when the freshest producer was started",
		},
	)
	metricCertExpirationTime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_cert_expiration_time",
			Help: "Kafka producer certificat NotAfter",
		},
	)
	metricCaExpirationTime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_ca_expiration_time",
			Help: "Kafka producer CA NotAfter",
		},
	)
	metricKafkaEventsQueueLen = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_events_queue_len",
			Help: "Kafka driver events queue length",
		},
	)

	metricRDKafkaGlobal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rdkafka_global",
			Help: "librdkafka internal global metrics",
		},
		[]string{"producer_id", "metric"},
	)
	metricRDKafkaBroker = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rdkafka_broker",
			Help: "librdkafka internal broker metrics",
		},
		[]string{"producer_id", "metric", "broker", "window"},
	)
	metricRDKafkaTopic = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rdkafka_topic",
			Help: "librdkafka internal topic metrics",
		},
		[]string{"producer_id", "metric", "topic", "window"},
	)
	metricRDKafkaPartition = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rdkafka_partition",
			Help: "librdkafka internal partition metrics",
		},
		[]string{"producer_id", "metric", "topic", "partition"},
	)
)

func registerMetrics(config *openapi.AlgoRunnerConfig) {

	deploymentLabel = fmt.Sprintf("%s/%s", config.DeploymentOwner, config.DeploymentName)
	pipelineLabel = fmt.Sprintf("%s/%s", config.PipelineOwner, config.PipelineName)
	componentLabel = "algo"
	algoLabel = fmt.Sprintf("%s/%s", config.Owner, config.Name)
	algoVersionLabel = config.Version
	algoIndexLabel = strconv.Itoa(int(config.Index))

	prometheus.MustRegister(runnerRuntimeHistogram,
		algoRuntimeHistogram,
		msgBytesInputCounter,
		msgBytesOutputCounter,
		dataBytesInputCounter,
		dataBytesOutputCounter,
		retryCounter,
		dlqCounter,
		algoErrorCounter,
		runnerErrorCounter,
		msgOK,
		msgNOK,
		msgDropped,
		producerQueueLen,
		eventIgnored,
		msgInTransit,
		libRdKafkaVersion,
		lastProducerStartTime,
		metricCertExpirationTime,
		metricCaExpirationTime,
		metricKafkaEventsQueueLen,
		metricRDKafkaGlobal,
		metricRDKafkaBroker,
		metricRDKafkaTopic,
		metricRDKafkaPartition)

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

func (m *Metrics) PopulateRDKafkaMetrics(stats string) error {
	var parser fastjson.Parser
	values, err := parser.Parse(stats)
	if err != nil {
		return err
	}
	producerID := string(values.GetStringBytes("name"))
	//librdkafka global metrics
	for _, m := range rdGlobalMetrics {
		metricRDKafkaGlobal.With(prometheus.Labels{
			"metric":      m,
			"producer_id": producerID,
		}).Set(values.GetFloat64(m))
	}
	//librdkafka broker metrics
	values.GetObject("brokers").Visit(func(key []byte, v *fastjson.Value) {
		brokerID := string(v.GetStringBytes("name"))
		metricRDKafkaBroker.With(prometheus.Labels{
			"metric":      "state_up",
			"producer_id": producerID,
			"broker":      brokerID,
			"window":      "",
		}).Set(B2f(string(v.GetStringBytes("state")) == "UP"))
		for _, m := range rdBrokerMetrics {
			metricRDKafkaBroker.With(prometheus.Labels{
				"metric":      m,
				"producer_id": producerID,
				"broker":      brokerID,
				"window":      "",
			}).Set(v.GetFloat64(m))
		}
		for _, m := range rdBrokerHistoMetrics {
			for _, window := range rdHistoMetrics {
				metricRDKafkaBroker.With(prometheus.Labels{
					"metric":      m,
					"producer_id": producerID,
					"broker":      brokerID,
					"window":      window,
				}).Set(v.GetFloat64(m, window))
			}
		}
	})
	//librdkafka topic metrics
	values.GetObject("topics").Visit(func(key []byte, v *fastjson.Value) {
		topic := string(v.GetStringBytes("topic"))
		for _, m := range rdTopicMetrics {
			for _, window := range rdHistoMetrics {
				metricRDKafkaTopic.With(prometheus.Labels{
					"metric":      m,
					"producer_id": producerID,
					"topic":       topic,
					"window":      window,
				}).Set(v.GetFloat64(m, window))
			}
		}
		//librdkafka topic-partition metrics
		for _, m := range rdPartitionMetrics {
			v.GetObject("partitions").Visit(func(key []byte, pv *fastjson.Value) {
				metricRDKafkaPartition.With(prometheus.Labels{
					"metric":      m,
					"producer_id": producerID,
					"topic":       topic,
					"partition":   string(key),
				}).Set(pv.GetFloat64(m))
			})
		}
	})
	return nil
}

func B2f(b bool) float64 {
	if b {
		return float64(1)
	}
	return float64(0)
}
