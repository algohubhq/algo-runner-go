package config

import (
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/openapi"
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type ConfigLoader struct {
	Logger *logging.Logger
}

// NewConfigLoader returns a new ConfigLoader struct
func NewConfigLoader(logger *logging.Logger) ConfigLoader {
	return ConfigLoader{
		Logger: logger,
	}
}

func (cl *ConfigLoader) LoadConfigFromFile(fileName string) openapi.AlgoRunnerConfig {

	raw, err := ioutil.ReadFile(fileName)
	if err != nil {
		cl.Logger.LogMessage.Msg = fmt.Sprintf("Unable to read the config file [%s].", fileName)
		cl.Logger.Log(err)
	}

	var c openapi.AlgoRunnerConfig
	jsonErr := json.Unmarshal(raw, &c)

	if jsonErr != nil {
		cl.Logger.LogMessage.Msg = fmt.Sprintf("Unable to deserialize the config file [%s].", fileName)
		cl.Logger.Log(jsonErr)
	}

	return c

}

func (cl *ConfigLoader) LoadConfigFromString(jsonConfig string) openapi.AlgoRunnerConfig {

	var c openapi.AlgoRunnerConfig
	jsonErr := json.Unmarshal([]byte(jsonConfig), &c)

	if jsonErr != nil {
		cl.Logger.LogMessage.Msg = fmt.Sprintf("Unable to deserialize the config from string [%s].", jsonConfig)
		cl.Logger.Log(jsonErr)
	}

	return c

}
