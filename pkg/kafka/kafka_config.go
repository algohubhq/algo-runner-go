package kafka

import (
	"algo-runner-go/pkg/openapi"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/common/log"
)

type KafkaConfig struct {
	config              *openapi.AlgoRunnerConfig
	KafkaConsumerConfig kafka.ConfigMap
	KafkaProducerConfig kafka.ConfigMap

	kafkaBrokers       string
	kafkaTLS           bool
	kafkaTLSCaLocation string

	kafkaAuthType string

	kafkaTLSUserLocation string
	kafkaTLSKeyLocation  string

	kafkaUsername string
	kafkaPassword string
}

// NewKafkaConfig returns a new KafkaConfig struct
func NewKafkaConfig(config *openapi.AlgoRunnerConfig,
	kafkaBrokers string) *KafkaConfig {

	kafkaConfig := &KafkaConfig{
		config:       config,
		kafkaBrokers: kafkaBrokers,
	}

	kafkaConfig.populateTLSConfig()
	kafkaConfig.populateAuthConfig()

	kafkaConfig.KafkaConsumerConfig = kafkaConfig.setKafkaConsumerConfigMap()
	kafkaConfig.KafkaProducerConfig = kafkaConfig.setKafkaProducerConfigMap()

	return kafkaConfig

}

// CheckForKafkaTLS checks for the KAFKA_TLS envar and certs
func (k *KafkaConfig) populateTLSConfig() bool {

	// Try to load from environment variable
	kafkaTLSEnv := os.Getenv("KAFKA_TLS")
	if kafkaTLSEnv == "" {
		return false
	}

	kafkaTLS, err := strconv.ParseBool(kafkaTLSEnv)
	if err != nil {
		return false
	}

	k.kafkaTLS = kafkaTLS

	if kafkaTLS {
		kafkaTLSCaLocationEnv := os.Getenv("KAFKA_TLS_CA_LOCATION")
		if kafkaTLSCaLocationEnv != "" {
			k.kafkaTLSCaLocation = kafkaTLSCaLocationEnv
		} else {
			k.kafkaTLSCaLocation = "/etc/ssl/certs/kafka-ca.crt"
		}

		// Be sure the certs exist
		if !fileExists(k.kafkaTLSCaLocation) {
			log.Error(err, fmt.Sprintf("KAFKA_TLS Enabled but no %s (ca crt) file exists", k.kafkaTLSCaLocation))
			return false
		}

	}

	return kafkaTLS

}

// populateAuthConfig checks for the Kafka Auth Environment variables
func (k *KafkaConfig) populateAuthConfig() string {

	// Try to load from environment variable
	kafkaAuthTypeEnv := os.Getenv("KAFKA_AUTH_TYPE")
	if kafkaAuthTypeEnv == "" {
		return ""
	}

	k.kafkaAuthType = kafkaAuthTypeEnv

	if kafkaAuthTypeEnv == "tls" {
		kafkaTLSUserLocationEnv := os.Getenv("KAFKA_AUTH_TLS_USER_LOCATION")
		if kafkaTLSUserLocationEnv != "" {
			k.kafkaTLSUserLocation = kafkaTLSUserLocationEnv
		} else {
			k.kafkaTLSUserLocation = "/etc/ssl/certs/kafka-user.crt"
		}

		kafkaTLSKeyLocationEnv := os.Getenv("KAFKA_AUTH_TLS_KEY_LOCATION")
		if kafkaTLSKeyLocationEnv != "" {
			k.kafkaTLSKeyLocation = kafkaTLSKeyLocationEnv
		} else {
			k.kafkaTLSKeyLocation = "/etc/ssl/certs/kafka-user.key"
		}

		if !fileExists(k.kafkaTLSUserLocation) {
			log.Error(errors.New("Failed to create Kafka Auth"), fmt.Sprintf("KAFKA_AUTH_TYPE set to tls but no %s (user crt) file exists", k.kafkaTLSUserLocation))
			return ""
		}
		if !fileExists(k.kafkaTLSKeyLocation) {
			log.Error(errors.New("Failed to create Kafka Auth"), fmt.Sprintf("KAFKA_AUTH_TYPE set to tls but no %s (user key) file exists", k.kafkaTLSKeyLocation))
			return ""
		}
	}

	if kafkaAuthTypeEnv == "scram-sha-512" || kafkaAuthTypeEnv == "plain" {
		kafkaUsernameEnv := os.Getenv("KAFKA_AUTH_USERNAME")
		if kafkaUsernameEnv != "" {
			k.kafkaUsername = kafkaUsernameEnv
		} else {
			log.Error(errors.New("Failed to create Kafka Auth"), fmt.Sprintf("KAFKA_AUTH_TYPE set to %s but KAFKA_AUTH_USERNAME envvar is not set", kafkaAuthTypeEnv))
			return ""
		}

		kafkaPasswordEnv := os.Getenv("KAFKA_AUTH_PASSWORD")
		if kafkaPasswordEnv != "" {
			k.kafkaPassword = kafkaPasswordEnv
		} else {
			log.Error(errors.New("Failed to create Kafka Auth"), fmt.Sprintf("KAFKA_AUTH_TYPE set to %s but KAFKA_AUTH_PASSWORD envvar is not set", kafkaAuthTypeEnv))
			return ""
		}
	}

	return kafkaAuthTypeEnv

}

func (k *KafkaConfig) setKafkaConsumerConfigMap() kafka.ConfigMap {

	groupID := fmt.Sprintf("algorun-%s-%s-%s-%s-%d",
		k.config.DeploymentOwner,
		k.config.DeploymentName,
		k.config.Owner,
		k.config.Name,
		k.config.Index,
	)

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers":        k.kafkaBrokers,
		"group.id":                 groupID,
		"client.id":                "algo-runner-go-client",
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
		"auto.offset.reset":        "earliest",
	}

	kafkaConfig = k.setKafkaTLSAuthMap(kafkaConfig)

	return kafkaConfig

}

func (k *KafkaConfig) setKafkaProducerConfigMap() kafka.ConfigMap {

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers":      k.kafkaBrokers,
		"statistics.interval.ms": 10000,
	}

	kafkaConfig = k.setKafkaTLSAuthMap(kafkaConfig)

	return kafkaConfig

}

func (k *KafkaConfig) setKafkaTLSAuthMap(kafkaConfig kafka.ConfigMap) kafka.ConfigMap {

	// Set the tls config
	if k.kafkaTLS {
		kafkaConfig["security.protocol"] = "ssl"
		kafkaConfig["ssl.ca.location"] = k.kafkaTLSCaLocation
	}
	// Set the auth config
	if k.kafkaAuthType == "tls" {
		kafkaConfig["security.protocol"] = "ssl"
		kafkaConfig["ssl.certificate.location"] = k.kafkaTLSUserLocation
		kafkaConfig["ssl.key.location"] = k.kafkaTLSKeyLocation
	}
	if k.kafkaAuthType == "scram-sha-512" {
		kafkaConfig["security.protocol"] = "SASL_SSL"
		kafkaConfig["sasl.mechanism"] = "SCRAM-SHA-256"
		kafkaConfig["sasl.username"] = k.kafkaTLSUserLocation
		kafkaConfig["sasl.password"] = k.kafkaTLSKeyLocation
	}
	if k.kafkaAuthType == "plain" {
		kafkaConfig["security.protocol"] = "SASL_PLAINTEXT"
		kafkaConfig["sasl.mechanism"] = "PLAIN"
		kafkaConfig["sasl.username"] = k.kafkaTLSUserLocation
		kafkaConfig["sasl.password"] = k.kafkaTLSKeyLocation
	}

	return kafkaConfig
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
