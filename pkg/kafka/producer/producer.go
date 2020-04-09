package kafkaproducer

import (
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	"algo-runner-go/pkg/types"
	"encoding/binary"
	"fmt"
	"strconv"

	k "algo-runner-go/pkg/kafka"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

type Producer struct {
	HealthyChan   chan<- bool
	Config        *openapi.AlgoRunnerConfig
	Logger        *logging.Logger
	Metrics       *metrics.Metrics
	InstanceName  string
	KafkaBrokers  string
	KafkaProducer *kafka.Producer
}

// NewProducer returns a new Producer struct
func NewProducer(healthyChan chan<- bool,
	config *openapi.AlgoRunnerConfig,
	instanceName string,
	kafkaBrokers string,
	logger *logging.Logger,
	metrics *metrics.Metrics) (producer *Producer, err error) {

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers":      kafkaBrokers,
		"statistics.interval.ms": 10000,
	}

	// Set the ssl config if enabled
	if k.CheckForKafkaTLS() {
		kafkaConfig["security.protocol"] = "ssl"
		kafkaConfig["ssl.ca.location"] = k.KafkaTLSCaLocation
		kafkaConfig["ssl.certificate.location"] = k.KafkaTLSUserLocation
		kafkaConfig["ssl.key.location"] = k.KafkaTLSKeyLocation
	}

	kp, err := kafka.NewProducer(&kafkaConfig)

	if err != nil {
		logger.Error("Failed to create Kafka message producer.", err)

		return nil, err
	}

	producer = &Producer{
		HealthyChan:   healthyChan,
		Config:        config,
		Logger:        logger,
		Metrics:       metrics,
		InstanceName:  instanceName,
		KafkaBrokers:  kafkaBrokers,
		KafkaProducer: kp,
	}

	go producer.producerEventsHandler()

	return producer, nil

}

func (p *Producer) producerEventsHandler() {
	for e := range p.KafkaProducer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			m := ev
			if m.TopicPartition.Error != nil {
				p.Metrics.MsgNOK.With(prometheus.Labels{
					"topic": *m.TopicPartition.Topic,
					"error": m.TopicPartition.Error.Error()}).Inc()
				p.Logger.Error(fmt.Sprintf("Delivery failed for output: %v", m.TopicPartition.Topic), m.TopicPartition.Error)
				// TODO: producer retry logic here

			} else {
				p.Metrics.MsgOK.With(prometheus.Labels{"topic": *m.TopicPartition.Topic}).Inc()
				p.Logger.Debug(fmt.Sprintf("Delivered message to topic %s [%d] at offset %v",
					*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset))
			}
		case kafka.Error:

			if ev.Code() == kafka.ErrAllBrokersDown {
				p.Logger.Error("All kafka brokers are down", ev)
			} else {
				p.Logger.Error("Kafka producer error", ev)
				p.HealthyChan <- false
			}

			// TODO: producer retry logic here

		case *kafka.Stats:
			err := p.Metrics.PopulateRDKafkaMetrics(ev.String())
			if err != nil {
				p.Logger.Error("Could not populate librdkafka metrics.", err)
			}
		default:
			p.Logger.Info(fmt.Sprintf("Ignored message: %v", ev))
			p.Metrics.EventIgnored.Inc()
		}
	}

	close(p.KafkaProducer.Events())

}

func (p *Producer) ProduceOutputMessage(traceID string,
	fileName string,
	topic string,
	outputName string,
	data []byte) {

	// Create the headers
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "fileName", Value: []byte(fileName)})
	headers = append(headers, kafka.Header{Key: "traceID", Value: []byte(traceID)})

	p.KafkaProducer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Headers:        headers,
		Key:            []byte(traceID),
		Value:          data,
	}

	p.Metrics.MsgBytesOutputCounter.WithLabelValues(p.Metrics.DeploymentLabel,
		p.Metrics.PipelineLabel,
		p.Metrics.ComponentLabel,
		p.Metrics.AlgoLabel,
		p.Metrics.AlgoVersionLabel,
		p.Metrics.AlgoIndexLabel,
		outputName,
		"ok").Add(float64(binary.Size(data)))

}

func (p *Producer) ProduceRetryMessage(processedMsg *types.ProcessedMsg,
	rawMessage *kafka.Message,
	topic string) {

	// Create the headers
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "traceID", Value: []byte(processedMsg.TraceID)})
	headers = append(headers, kafka.Header{Key: "contentType", Value: []byte(processedMsg.ContentType)})
	headers = append(headers, kafka.Header{Key: "fileName", Value: []byte(processedMsg.FileName)})
	headers = append(headers, kafka.Header{Key: "messageDataType", Value: []byte(processedMsg.MessageDataType)})
	headers = append(headers, kafka.Header{Key: "endpointParams", Value: []byte(processedMsg.EndpointParams)})
	headers = append(headers, kafka.Header{Key: "run", Value: []byte(strconv.FormatBool(processedMsg.Run))})

	headers = append(headers, kafka.Header{Key: "retryStepIndex", Value: []byte(strconv.Itoa(processedMsg.RetryStepIndex))})
	headers = append(headers, kafka.Header{Key: "retryNum", Value: []byte(strconv.Itoa(processedMsg.RetryNum))})
	headers = append(headers, kafka.Header{Key: "retryTimestamp", Value: []byte(strconv.Itoa(int(processedMsg.RetryTimestamp.Unix())))})

	p.KafkaProducer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Headers:        headers,
		Key:            []byte(processedMsg.TraceID),
		Value:          rawMessage.Value,
	}

	p.Metrics.MsgBytesOutputCounter.WithLabelValues(p.Metrics.DeploymentLabel,
		p.Metrics.PipelineLabel,
		p.Metrics.ComponentLabel,
		p.Metrics.AlgoLabel,
		p.Metrics.AlgoVersionLabel,
		p.Metrics.AlgoIndexLabel,
		"",
		"retry").Add(float64(binary.Size(rawMessage.Value)))

	p.Metrics.DataBytesOutputCounter.WithLabelValues(p.Metrics.DeploymentLabel,
		p.Metrics.PipelineLabel,
		p.Metrics.ComponentLabel,
		p.Metrics.AlgoLabel,
		p.Metrics.AlgoVersionLabel,
		p.Metrics.AlgoIndexLabel,
		"",
		"retry").Add(float64(binary.Size(rawMessage.Value)))

}

func (p *Producer) canRetry(err error) bool {
	switch e := err.(kafka.Error); e.Code() {
	// topics are wrong
	case kafka.ErrTopicException, kafka.ErrUnknownTopic:
		return false
	// message is incorrect
	case kafka.ErrMsgSizeTooLarge, kafka.ErrInvalidMsgSize:
		return false
	default:
		return true
	}
}
