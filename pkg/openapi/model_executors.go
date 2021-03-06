/*
 * Algo.Run API 1.0-beta1
 *
 * API for the Algo.Run Engine
 *
 * API version: 1.0-beta1
 * Contact: support@algohub.com
 * Generated by: OpenAPI Generator (https://openapi-generator.tech)
 */

package openapi
// Executors the model 'Executors'
type Executors string

// List of Executors
const (
	EXECUTORS_UNKNOWN Executors = "Unknown"
	EXECUTORS_EXECUTABLE Executors = "Executable"
	EXECUTORS_HTTP Executors = "http"
	EXECUTORS_GRPC Executors = "grpc"
	EXECUTORS_SPARK Executors = "Spark"
	EXECUTORS_KAFKA_STREAMS Executors = "KafkaStreams"
	EXECUTORS_KSQL Executors = "ksql"
	EXECUTORS_FAUST Executors = "Faust"
	EXECUTORS_DELEGATED Executors = "Delegated"
)
