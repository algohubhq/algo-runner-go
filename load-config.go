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
		RunnerLogData:  &swagger.RunnerLogData{},
	}

	raw, err := ioutil.ReadFile(fileName)
	if err != nil {
		localLog.Status = "Failed"
		localLog.RunnerLogData.Log = fmt.Sprintf("Unable to read the config file [%s] with error: %s\n", fileName, err)
		localLog.log()
	}

	var c swagger.RunnerConfig
	jsonErr := json.Unmarshal(raw, &c)

	if jsonErr != nil {
		localLog.Status = "Failed"
		localLog.RunnerLogData.Log = fmt.Sprintf("Unable to deserialize the config file [%s] with error: %s\n", fileName, jsonErr)
		localLog.log()
	}

	return c

}

func loadConfigFromString(jsonConfig string) swagger.RunnerConfig {

	// Create the base log message
	localLog := logMessage{
		LogMessageType: "Local",
		Status:         "Started",
		RunnerLogData:  &swagger.RunnerLogData{},
	}

	var c swagger.RunnerConfig
	jsonErr := json.Unmarshal([]byte(jsonConfig), &c)

	if jsonErr != nil {
		localLog.Status = "Failed"
		localLog.RunnerLogData.Log = fmt.Sprintf("Unable to deserialize the config from string [%s] with error: %s\n", jsonConfig, jsonErr)
		localLog.log()
	}

	return c

}
