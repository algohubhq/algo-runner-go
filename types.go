package main

import "algo-runner-go/swagger"

// InputData contains either the byte slice of raw data or the file name and path to the saved data
type InputData struct {
	isFileReference bool
	contentType     string
	fileReference   *swagger.FileReference
	data            []byte
}

type StorageConfig struct {
	connectionString string
	host             string
	accessKeyID      string
	secretAccessKey  string
	useSSL           bool
}
