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

type DataConnectorOptionModel struct {

	SortOrder int32 `json:"sortOrder,omitempty"`

	Name string `json:"name,omitempty"`

	Description string `json:"description,omitempty"`

	Value string `json:"value,omitempty"`

	DataType *DataTypeModel `json:"dataType,omitempty"`
}
