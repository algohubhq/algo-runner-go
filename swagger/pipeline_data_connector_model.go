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

type PipelineDataConnectorModel struct {

	DataConnector *DataConnectorModel `json:"dataConnector,omitempty"`

	FullName string `json:"fullName,omitempty"`

	DataConnectorVersionTag string `json:"dataConnectorVersionTag,omitempty"`

	Index int32 `json:"index,omitempty"`

	OptionOverrides []DataConnectorOptionModel `json:"optionOverrides,omitempty"`

	PositionX float32 `json:"positionX,omitempty"`

	PositionY float32 `json:"positionY,omitempty"`
}
