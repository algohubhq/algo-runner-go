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
// PipelineDataConnectorModel struct for PipelineDataConnectorModel
type PipelineDataConnectorModel struct {
	DataConnector *DataConnectorModel `json:"dataConnector,omitempty"`
	FullName string `json:"fullName,omitempty"`
	DataConnectorVersionTag string `json:"dataConnectorVersionTag,omitempty"`
	Index int32 `json:"index,omitempty"`
	ConfigMounts []ConfigMountModel `json:"configMounts,omitempty"`
	TopicConfigs []TopicConfigModel `json:"topicConfigs,omitempty"`
	OptionOverrides []DataConnectorOptionModel `json:"optionOverrides,omitempty"`
	LivenessProbe *ProbeV1 `json:"livenessProbe,omitempty"`
	ReadinessProbe *ProbeV1 `json:"readinessProbe,omitempty"`
	PositionX float32 `json:"positionX,omitempty"`
	PositionY float32 `json:"positionY,omitempty"`
}
