/*
 * Algo.Run API 1.0
 *
 * API for the Algo.Run Engine
 *
 * API version: 1.0
 * Contact: support@algohub.com
 * Generated by: Swagger Codegen (https://github.com/swagger-api/swagger-codegen.git)
 */

package swagger

import (
	"time"
)

type LogMessage struct {
	LogMessageType string `json:"logMessageType,omitempty"`

	LogTimestamp time.Time `json:"logTimestamp,omitempty"`

	RunId string `json:"runId,omitempty"`

	AlgoInstanceName string `json:"algoInstanceName,omitempty"`

	EndpointOwnerUserName string `json:"endpointOwnerUserName,omitempty"`

	EndpointName string `json:"endpointName,omitempty"`

	AlgoOwnerUserName string `json:"algoOwnerUserName,omitempty"`

	AlgoName string `json:"algoName,omitempty"`

	AlgoVersionTag string `json:"algoVersionTag,omitempty"`

	AlgoIndex int32 `json:"algoIndex,omitempty"`

	Status string `json:"status,omitempty"`

	CompletionPercent float64 `json:"completionPercent,omitempty"`

	RuntimeMs int64 `json:"runtimeMs,omitempty"`

	Log string `json:"log,omitempty"`
}
