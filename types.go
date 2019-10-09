package main

// InputData contains either the byte slice of raw data or the file name and path to the saved data
type InputData struct {
	isFileReference bool
	contentType     string
	fileName        string
	filePath        string
	data            []byte
}

type S3Config struct {
	connectionString string `json:"connectionString"`
	host             string `json:"host"`
	accessKeyID      string `json:"accessKeyID"`
	secretAccessKey  string `json:"secretAccessKey"`
	useSSL           bool   `json:"useSSL"`
}
