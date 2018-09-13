package main

import (
	"algo-runner-go/swagger"
	"encoding/json"
	"fmt"
	"io/ioutil"
)

func loadConfigFromFile(fileName string) swagger.RunnerConfig {

	// Create the base log message
	localLog := logMessage{
		LogMessageType: "Local",
		Status:         "Started",
	}

	raw, err := ioutil.ReadFile(fileName)
	if err != nil {
		localLog.log("Failed", fmt.Sprintf("Unable to read the config file [%s] with error: %s\n", fileName, err))
	}

	var c swagger.RunnerConfig
	jsonErr := json.Unmarshal(raw, &c)

	if jsonErr != nil {
		localLog.log("Failed", fmt.Sprintf("Unable to deserialize the config file [%s] with error: %s\n", fileName, jsonErr))
	}

	return c

}

func loadConfigFromString(jsonConfig string) swagger.RunnerConfig {

	// Create the base log message
	localLog := logMessage{
		LogMessageType: "Local",
		Status:         "Started",
	}

	var c swagger.RunnerConfig
	jsonErr := json.Unmarshal([]byte(jsonConfig), &c)

	if jsonErr != nil {
		localLog.log("Failed", fmt.Sprintf("Unable to deserialize the config from string [%s] with error: %s\n", jsonConfig, jsonErr))
	}

	return c

}
