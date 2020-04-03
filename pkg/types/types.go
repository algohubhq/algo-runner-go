package types

import (
	"algo-runner-go/pkg/openapi"
)

// IRunner is an interface to define the functions of a runner
type IRunner interface {
	Run(traceID string,
		endpointParams string,
		inputMap map[*openapi.AlgoInputModel][]InputData) error
}

// InputData contains either the byte slice of raw data or the file name and path to the saved data
type InputData struct {
	IsFileReference bool
	ContentType     string
	FileReference   *openapi.FileReference
	Data            []byte
	MsgSize         float64
	DataSize        float64
}
