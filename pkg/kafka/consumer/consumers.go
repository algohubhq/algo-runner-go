package kafkaconsumer

import (
	k "algo-runner-go/pkg/kafka"
	kafkaproducer "algo-runner-go/pkg/kafka/producer"
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	"algo-runner-go/pkg/storage"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumers struct {
	HealthyChan   chan<- bool
	Config        *openapi.AlgoRunnerConfig
	Producer      *kafkaproducer.Producer
	StorageConfig *storage.Storage
	Logger        *logging.Logger
	Metrics       *metrics.Metrics
	InstanceName  string
	KafkaBrokers  string
	Consumers     []*Consumer
}

// NewConsumers returns a new Consumer struct
func NewConsumers(healthyChan chan<- bool,
	config *openapi.AlgoRunnerConfig,
	producer *kafkaproducer.Producer,
	storageConfig *storage.Storage,
	instanceName string,
	kafkaBrokers string,
	logger *logging.Logger,
	metrics *metrics.Metrics) (*Consumers, error) {

	consumers := Consumers{
		HealthyChan:   healthyChan,
		Config:        config,
		Producer:      producer,
		StorageConfig: storageConfig,
		Logger:        logger,
		Metrics:       metrics,
		InstanceName:  instanceName,
		KafkaBrokers:  kafkaBrokers,
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

	algoName := fmt.Sprintf("%s/%s:%s[%d]", c.Config.AlgoOwnerUserName, c.Config.AlgoName, c.Config.AlgoVersionTag, c.Config.AlgoIndex)

	for _, pipe := range c.Config.Pipes {

		if pipe.DestName == algoName {

			var input openapi.AlgoInputModel
			// Get the input associated with this route
			for i := range c.Config.Inputs {
				if c.Config.Inputs[i].Name == pipe.DestInputName {
					input = c.Config.Inputs[i]
					break
				}
			}

			var topicConfig openapi.TopicConfigModel
			// Get the topic c.Config associated with this route
			for x := range c.Config.TopicConfigs {
				if c.Config.TopicConfigs[x].SourceName == pipe.SourceName &&
					c.Config.TopicConfigs[x].SourceOutputName == pipe.SourceOutputName {
					topicConfig = c.Config.TopicConfigs[x]
					break
				}
			}

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
							c.Logger.LogMessage.Msg = fmt.Sprintf("Error parsing backoff duration to calculate max.poll.interval.ms [%s]", s.BackoffDuration)
							c.Logger.Log(err)
						}
						maxPollIntervalMs = Max(maxPollIntervalMs, int(d.Milliseconds())+300000)
					}
				}
			}
			mainKafkaConfig := c.getKafkaConfigMap()
			// Set the max poll interval
			mainKafkaConfig["max.poll.interval.ms"] = int(maxPollIntervalMs)

			// Replace the deployment username and name in the topic string
			topicName := strings.ToLower(strings.Replace(topicConfig.TopicName, "{deploymentownerusername}", c.Config.DeploymentOwnerUserName, -1))
			topicName = strings.ToLower(strings.Replace(topicName, "{deploymentname}", c.Config.DeploymentName, -1))

			kc, err := kafka.NewConsumer(&mainKafkaConfig)
			if err != nil {
				c.HealthyChan <- false
				c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to create consumer.")
				c.Logger.Log(err)
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
				c.KafkaBrokers,
				topicName,
				topicName,
				c.Logger,
				c.Metrics)

			c.Consumers = append(c.Consumers, &mainConsumer)

			if c.Config.TopicRetryEnabled {
				if *retryStrategy.Strategy == openapi.RETRYSTRATEGIES_RETRY_TOPICS {
					for _, step := range retryStrategy.Steps {

						d, err := time.ParseDuration(step.BackoffDuration)
						if err != nil {
							c.Logger.LogMessage.Msg = fmt.Sprintf("Error parsing backoff duration to calculate max.poll.interval.ms [%s]", step.BackoffDuration)
							c.Logger.Log(err)
						}
						maxPollIntervalMs = Max(maxPollIntervalMs, int(d.Milliseconds())+300000)

						retryKafkaConfig := c.getKafkaConfigMap()
						// Set the max poll interval
						retryKafkaConfig["max.poll.interval.ms"] = maxPollIntervalMs

						retryTopicName := strings.ToLower(fmt.Sprintf("%s.%s", topicName, step.BackoffDuration))

						kc, err := kafka.NewConsumer(&retryKafkaConfig)
						if err != nil {
							c.HealthyChan <- false
							c.Logger.LogMessage.Msg = fmt.Sprintf("Failed to create consumer.")
							c.Logger.Log(err)
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
							c.KafkaBrokers,
							topicName,
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

func (c *Consumers) getKafkaConfigMap() kafka.ConfigMap {

	groupID := fmt.Sprintf("algorun-%s-%s-%s-%s-%d-dev",
		c.Config.DeploymentOwnerUserName,
		c.Config.DeploymentName,
		c.Config.AlgoOwnerUserName,
		c.Config.AlgoName,
		c.Config.AlgoIndex,
	)

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers":        c.KafkaBrokers,
		"group.id":                 groupID,
		"client.id":                "algo-runner-go-client",
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
		"auto.offset.reset":        "earliest",
	}

	// Set the ssl c.Config if enabled
	if k.CheckForKafkaTLS() {
		kafkaConfig["security.protocol"] = "ssl"
		kafkaConfig["ssl.ca.location"] = k.KafkaTLSCaLocation
		kafkaConfig["ssl.certificate.location"] = k.KafkaTLSUserLocation
		kafkaConfig["ssl.key.location"] = k.KafkaTLSKeyLocation
	}

	return kafkaConfig

}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}
