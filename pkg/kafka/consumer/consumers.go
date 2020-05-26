package kafkaconsumer

import (
	k "algo-runner-go/pkg/kafka"
	kafkaproducer "algo-runner-go/pkg/kafka/producer"
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	"algo-runner-go/pkg/storage"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumers struct {
	HealthyChan   chan<- bool
	Config        *openapi.AlgoRunnerConfig
	kafkaConfig   *k.KafkaConfig
	Producer      *kafkaproducer.Producer
	StorageConfig *storage.Storage
	Logger        *logging.Logger
	Metrics       *metrics.Metrics
	InstanceName  string
	Consumers     []*Consumer
}

// NewConsumers returns a new Consumer struct
func NewConsumers(healthyChan chan<- bool,
	config *openapi.AlgoRunnerConfig,
	kafkaConfig *k.KafkaConfig,
	producer *kafkaproducer.Producer,
	storageConfig *storage.Storage,
	instanceName string,
	logger *logging.Logger,
	metrics *metrics.Metrics) (*Consumers, error) {

	consumers := Consumers{
		HealthyChan:   healthyChan,
		Config:        config,
		kafkaConfig:   kafkaConfig,
		Producer:      producer,
		StorageConfig: storageConfig,
		Logger:        logger,
		Metrics:       metrics,
		InstanceName:  instanceName,
	}

	err := consumers.createConsumers()

	return &consumers, err

}

func (c *Consumers) Start() {
	for _, consumer := range c.Consumers {
		consumer.Start()
	}
}

func (c *Consumers) createConsumers() error {

	algoName := fmt.Sprintf("%s/%s:%s[%d]", c.Config.Owner, c.Config.Name, c.Config.Version, c.Config.Index)

	for _, pipe := range c.Config.Pipes {

		if pipe.DestName == algoName {

			var input openapi.AlgoInputSpec
			// Get the input associated with this route
			for i := range c.Config.Inputs {
				if c.Config.Inputs[i].Name == pipe.DestInputName {
					input = c.Config.Inputs[i]
					break
				}
			}

			// Get the topic Config associated with this route
			topicConfig := c.Config.Topics[fmt.Sprintf("%s|%s", pipe.SourceName, pipe.SourceOutputName)]

			retryStrategy := c.Config.RetryStrategy
			if topicConfig.OverrideRetryStrategy {
				retryStrategy = topicConfig.RetryStrategy
			}

			// Ensure the steps are sorted by index
			var maxPollIntervalMs = int(300000)
			if retryStrategy != nil && retryStrategy.Steps != nil {
				sort.SliceStable(retryStrategy.Steps, func(i, j int) bool {
					return retryStrategy.Steps[i].Index < retryStrategy.Steps[j].Index
				})
				// if simple retry strategy, get the max backoff duration
				// This is used to increase the max.poll.interval.ms for the consumer
				if *retryStrategy.Strategy == openapi.RETRYSTRATEGIES_SIMPLE {
					for _, s := range retryStrategy.Steps {
						d, err := time.ParseDuration(s.BackoffDuration)
						if err != nil {
							c.Logger.Error(fmt.Sprintf("Error parsing backoff duration to calculate max.poll.interval.ms [%s]", s.BackoffDuration), err)
						}
						maxPollIntervalMs = Max(maxPollIntervalMs, int(d.Milliseconds())+300000)
					}
				}
			}

			mainKafkaConfig := c.kafkaConfig.KafkaConsumerConfig
			// Set the max poll interval
			mainKafkaConfig["max.poll.interval.ms"] = int(maxPollIntervalMs)

			if topicConfig.TopicName == "" {
				c.HealthyChan <- false
				msg := fmt.Sprintf("No Topic config defined for output %s %s", pipe.SourceName, pipe.SourceOutputName)
				err := errors.New("Missing Topic Config")
				c.Logger.Error(msg, err)
				return err
			}

			kc, err := kafka.NewConsumer(&mainKafkaConfig)
			if err != nil {
				c.HealthyChan <- false
				c.Logger.Error("Failed to create consumer.", err)
				return err
			}

			mainConsumer := NewConsumer(c.HealthyChan,
				c.Config,
				kc,
				&input,
				retryStrategy,
				nil,
				c.Producer,
				c.StorageConfig,
				c.InstanceName,
				topicConfig.TopicName,
				topicConfig.TopicName,
				c.Logger,
				c.Metrics)

			c.Consumers = append(c.Consumers, &mainConsumer)

			if c.Config.RetryEnabled {
				if *retryStrategy.Strategy == openapi.RETRYSTRATEGIES_RETRY_TOPICS {
					for _, step := range retryStrategy.Steps {

						d, err := time.ParseDuration(step.BackoffDuration)
						if err != nil {
							c.Logger.Error(fmt.Sprintf("Error parsing backoff duration to calculate max.poll.interval.ms [%s]", step.BackoffDuration), err)
						}
						maxPollIntervalMs = Max(maxPollIntervalMs, int(d.Milliseconds())+300000)

						retryKafkaConfig := c.kafkaConfig.KafkaConsumerConfig
						// Set the max poll interval
						retryKafkaConfig["max.poll.interval.ms"] = maxPollIntervalMs

						retryTopicName := strings.ToLower(fmt.Sprintf("%s.%s", topicConfig.TopicName, step.BackoffDuration))

						kc, err := kafka.NewConsumer(&retryKafkaConfig)
						if err != nil {
							c.HealthyChan <- false
							c.Logger.Error("Failed to create consumer.", err)
							return err
						}
						consumer := NewConsumer(c.HealthyChan,
							c.Config,
							kc,
							&input,
							retryStrategy,
							&step,
							c.Producer,
							c.StorageConfig,
							c.InstanceName,
							topicConfig.TopicName,
							retryTopicName,
							c.Logger,
							c.Metrics)

						c.Consumers = append(c.Consumers, &consumer)

					}
				}
			}

		}

	}

	return nil

}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}
