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

type RunnerConfig struct {

	EndpointOwnerUserName string `json:"endpointOwnerUserName,omitempty"`

	EndpointName string `json:"endpointName,omitempty"`

	PipelineOwnerUserName string `json:"pipelineOwnerUserName,omitempty"`

	PipelineName string `json:"pipelineName,omitempty"`

	AlgoOwnerUserName string `json:"algoOwnerUserName,omitempty"`

	AlgoName string `json:"algoName,omitempty"`

	AlgoVersionTag string `json:"algoVersionTag,omitempty"`

	AlgoIndex int32 `json:"algoIndex,omitempty"`

	Entrypoint string `json:"entrypoint,omitempty"`

	ServerType string `json:"serverType,omitempty"`

	AlgoParams []AlgoParamModel `json:"algoParams,omitempty"`

	Inputs []AlgoInputModel `json:"inputs,omitempty"`

	Outputs []AlgoOutputModel `json:"outputs,omitempty"`

	WriteAllOutputs bool `json:"writeAllOutputs,omitempty"`

	Pipes []PipeModel `json:"pipes,omitempty"`

	TopicConfigs []TopicConfigModel `json:"topicConfigs,omitempty"`

	GpuEnabled bool `json:"gpuEnabled,omitempty"`

	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}
