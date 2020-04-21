package kafka

import (
	"fmt"
	"os"
	"strconv"

	"github.com/prometheus/common/log"
)

var kafkaTLS bool
var KafkaTLSCaLocation, KafkaTLSUserLocation, KafkaTLSKeyLocation string

// CheckForKafkaTLS checks for the KAFKA_TLS envar and certs
func CheckForKafkaTLS() bool {

	// Try to load from environment variable
	kafkaTLSEnv := os.Getenv("KAFKA_TLS")
	if kafkaTLSEnv == "" {
		return false
	}

	kafkaTLS, err := strconv.ParseBool(kafkaTLSEnv)
	if err != nil {
		return false
	}

	if kafkaTLS {

		kafkaTLSCaLocationEnv := os.Getenv("KAFKA_TLS_CA_LOCATION")
		if kafkaTLSCaLocationEnv != "" {
			KafkaTLSCaLocation = kafkaTLSCaLocationEnv
		} else {
			KafkaTLSCaLocation = "/etc/ssl/certs/kafka-ca.crt"
		}

		kafkaTLSUserLocationEnv := os.Getenv("KAFKA_TLS_USER_LOCATION")
		if kafkaTLSUserLocationEnv != "" {
			KafkaTLSUserLocation = kafkaTLSUserLocationEnv
		} else {
			KafkaTLSUserLocation = "/etc/ssl/certs/kafka-user.crt"
		}

		kafkaTLSKeyLocationEnv := os.Getenv("KAFKA_TLS_KEY_LOCATION")
		if kafkaTLSKeyLocationEnv != "" {
			KafkaTLSKeyLocation = kafkaTLSKeyLocationEnv
		} else {
			KafkaTLSKeyLocation = "/etc/ssl/certs/kafka-user.key"
		}

		// Be sure the certs exist
		if !fileExists(KafkaTLSCaLocation) {
			log.Error(err, fmt.Sprintf("KAFKA-TLS Enabled but no %s (ca crt) file exists", KafkaTLSCaLocation))
			return false
		}
		if !fileExists(KafkaTLSUserLocation) {
			log.Error(err, fmt.Sprintf("KAFKA-TLS Enabled but no %s (user crt) file exists", KafkaTLSUserLocation))
			return false
		}
		if !fileExists(KafkaTLSKeyLocation) {
			log.Error(err, fmt.Sprintf("KAFKA-TLS Enabled but no %s (user key) file exists", KafkaTLSKeyLocation))
			return false
		}
	}

	return kafkaTLS

}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
