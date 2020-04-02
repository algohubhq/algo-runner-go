package kafkaproducer

import (
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	"fmt"

	k "algo-runner-go/pkg/kafka"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	HealthyChan  chan<- bool
	Config       *openapi.AlgoRunnerConfig
	Logger       *logging.Logger
	Metrics      *metrics.Metrics
	InstanceName string
	KafkaBrokers string
}

// NewProducer returns a new Producer struct
func NewProducer(healthyChan chan<- bool,
	config *openapi.AlgoRunnerConfig,
	instanceName string,
	kafkaBrokers string,
	logger *logging.Logger,
	metrics *metrics.Metrics) Producer {

	return Producer{
		HealthyChan:  healthyChan,
		Config:       config,
		Logger:       logger,
		Metrics:      metrics,
		InstanceName: instanceName,
		KafkaBrokers: kafkaBrokers,
	}
}

func (p *Producer) ProduceOutputMessage(traceID string, fileName string, topic string, data []byte) {

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers": p.KafkaBrokers,
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
		p.Logger.LogMessage.Type = "Local"
		p.Logger.LogMessage.Msg = "Failed to create Kafka message producer."
		p.Logger.Log(err)

		return
	}

	doneChan := make(chan bool)

	go func() {
		defer close(doneChan)
		for e := range kp.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					p.Logger.LogMessage.Type = "Runner"
					p.Logger.LogMessage.Msg = fmt.Sprintf("Delivery failed for output: %v", m.TopicPartition.Topic)
					p.Logger.Log(m.TopicPartition.Error)
				} else {
					p.Logger.LogMessage.Type = "Runner"
					p.Logger.LogMessage.Msg = fmt.Sprintf("Delivered message to topic %s [%d] at offset %v",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					p.Logger.Log(nil)
				}
				return
			case kafka.Error:
				p.Logger.LogMessage.Msg = fmt.Sprintf("Failed to deliver output message to Kafka: %v", e)
				p.Logger.Log(nil)
				p.HealthyChan <- false

			default:
				p.Logger.LogMessage.Type = "Local"
				p.Logger.LogMessage.Msg = fmt.Sprintf("Ignored event: %s", ev)
				p.Logger.Log(nil)
			}
		}
	}()

	// Create the headers
	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "fileName", Value: []byte(fileName)})

	kp.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: []byte(traceID), Value: data}

	// wait for delivery report goroutine to finish
	_ = <-doneChan

	kp.Close()

}
