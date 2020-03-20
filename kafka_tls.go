package main

import (
	"fmt"
	"os"
	"strconv"
)

var kafkaTLS bool
var kafkaTLSCaLocation, kafkaTLSUserLocation, kafkaTLSKeyLocation string

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
			kafkaTLSCaLocation = kafkaTLSCaLocationEnv
		} else {
			kafkaTLSCaLocation = "/etc/ssl/certs/kafka-ca.crt"
		}

		kafkaTLSUserLocationEnv := os.Getenv("KAFKA_TLS_USER_LOCATION")
		if kafkaTLSUserLocationEnv != "" {
			kafkaTLSUserLocation = kafkaTLSUserLocationEnv
		} else {
			kafkaTLSUserLocation = "/etc/ssl/certs/kafka-user.crt"
		}

		kafkaTLSKeyLocationEnv := os.Getenv("KAFKA_TLS_KEY_LOCATION")
		if kafkaTLSKeyLocationEnv != "" {
			kafkaTLSKeyLocation = kafkaTLSKeyLocationEnv
		} else {
			kafkaTLSKeyLocation = "/etc/ssl/certs/kafka-user.key"
		}

		// Be sure the certs exist
		if !fileExists(kafkaTLSCaLocation) {
			log.Error(err, fmt.Sprintf("KAFKA-TLS Enabled but no %s (ca crt) file exists", kafkaTLSCaLocation))
			return false
		}
		if !fileExists(kafkaTLSUserLocation) {
			log.Error(err, fmt.Sprintf("KAFKA-TLS Enabled but no %s (user crt) file exists", kafkaTLSUserLocation))
			return false
		}
		if !fileExists(kafkaTLSKeyLocation) {
			log.Error(err, fmt.Sprintf("KAFKA-TLS Enabled but no %s (user key) file exists", kafkaTLSKeyLocation))
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
