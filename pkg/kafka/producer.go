package kafka

import (
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	Config       *openapi.AlgoRunnerConfig
	Logger       *logging.Logger
	Metrics      *metrics.Metrics
	InstanceName string
}

// NewProducer returns a new Producer struct
func NewProducer(config *openapi.AlgoRunnerConfig,
	logger *logging.Logger,
	metrics *metrics.Metrics) Producer {

	return Producer{
		Config:  config,
		Logger:  logger,
		Metrics: metrics,
	}
}

func (p *Producer) ProduceOutputMessage(traceID string, fileName string, topic string, data []byte) {

	// Create the base log message
	runnerLog := openapi.LogEntryModel{
		Type:    "Runner",
		Version: "1",
		Data: map[string]interface{}{
			"DeploymentOwnerUserName": p.Config.DeploymentOwnerUserName,
			"DeploymentName":          p.Config.DeploymentName,
			"AlgoOwnerUserName":       p.Config.AlgoOwnerUserName,
			"AlgoName":                p.Config.AlgoName,
			"AlgoVersionTag":          p.Config.AlgoVersionTag,
			"AlgoInstanceName":        p.InstanceName,
		},
	}

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers": *kafkaBrokers,
	}

	// Set the ssl config if enabled
	if CheckForKafkaTLS() {
		kafkaConfig["security.protocol"] = "ssl"
		kafkaConfig["ssl.ca.location"] = kafkaTLSCaLocation
		kafkaConfig["ssl.certificate.location"] = kafkaTLSUserLocation
		kafkaConfig["ssl.key.location"] = kafkaTLSKeyLocation
	}

	kp, err := kafka.NewProducer(&kafkaConfig)

	if err != nil {
		runnerLog.Type = "Local"
		runnerLog.Msg = "Failed to create Kafka message producer."
		runnerLog.log(err)

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
					runnerLog.Type = "Runner"
					runnerLog.Msg = fmt.Sprintf("Delivery failed for output: %v", m.TopicPartition.Topic)
					runnerLog.log(m.TopicPartition.Error)
				} else {
					runnerLog.Type = "Runner"
					runnerLog.Msg = fmt.Sprintf("Delivered message to topic %s [%d] at offset %v",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					runnerLog.log(nil)
				}
				return
			case kafka.Error:
				runnerLog.Msg = fmt.Sprintf("Failed to deliver output message to Kafka: %v", e)
				runnerLog.log(nil)
				healthy = false

			default:

				runnerLog.Type = "Local"
				runnerLog.Msg = fmt.Sprintf("Ignored event: %s", ev)
				runnerLog.log(nil)
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
