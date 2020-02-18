package main

import (
	"os"
	"strconv"
)

// CheckForKafkaTLS checks for the KAFKA-TLS envar and certs
func CheckForKafkaTLS() bool {

	// Try to load from environment variable
	kafkaTLSEnv := os.Getenv("KAFKA-TLS")
	if kafkaTLSEnv == "" {
		return false
	}

	kafkaTLS, err := strconv.ParseBool(kafkaTLSEnv)
	if err != nil {
		return false
	}

	if kafkaTLS {
		// Be sure the certs exist
		if !fileExists("/var/run/secrets/algo.run/kafka-ca.crt") {
			log.Error(err, "KAFKA-TLS Enabled but no /var/run/secrets/algo.run/kafka-ca.crt file exists")
			return false
		}
		if !fileExists("/var/run/secrets/algo.run/kafka-user.crt") {
			log.Error(err, "KAFKA-TLS Enabled but no /var/run/secrets/algo.run/kafka-user.crt file exists")
			return false
		}
		if !fileExists("/var/run/secrets/algo.run/kafka-user.key") {
			log.Error(err, "KAFKA-TLS Enabled but no /var/run/secrets/algo.run/kafka-user.key file exists")
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
