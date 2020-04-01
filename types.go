package main

import "algo-runner-go/pkg/openapi"

// InputData contains either the byte slice of raw data or the file name and path to the saved data
type InputData struct {
	isFileReference bool
	contentType     string
	fileReference   *openapi.FileReference
	data            []byte
}

// StorageConfig defines the S3 compatible storage configuration
type StorageConfig struct {
	connectionString string
	host             string
	accessKeyID      string
	secretAccessKey  string
	useSSL           bool
}
