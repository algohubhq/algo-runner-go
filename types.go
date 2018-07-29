package main

// InputData contains either the byte slice of raw data or the file name and path to the saved data
type InputData struct {
	isFileReference bool
	contentType     string
	fileName        string
	filePath        string
	data            []byte
}

// FileReference the path and filename
type FileReference struct {
	FilePath string `json:"filePath"`
	FileName string `json:"fileName"`
}
