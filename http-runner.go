package main

import (
	"algo-runner-go/swagger"
	"bytes"
	"errors"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func runHTTP(runID string,
	inputMap map[*swagger.AlgoInputModel][]InputData,
	algoIndex int32) (err error) {

	startTime := time.Now()

	// Create the base message
	algoLog := swagger.LogMessage{
		LogMessageType:        "Algo",
		EndpointOwnerUserName: config.EndpointOwnerUserName,
		EndpointName:          config.EndpointName,
		AlgoOwnerUserName:     config.AlgoOwnerUserName,
		AlgoName:              config.AlgoName,
		AlgoVersionTag:        config.AlgoVersionTag,
		AlgoIndex:             algoIndex,
		Status:                "Started",
	}

	// TODO: Write to the topic as error if no value
	if inputMap == nil {

		// 	return
	}

	var netClient = &http.Client{
		Timeout: time.Second * 10,
	}

	// Set the timeout
	if config.TimeoutSeconds > 0 {
		netClient.Timeout = time.Second * time.Duration(config.TimeoutSeconds)
	}

	outputTopic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.%d.output.default",
		config.EndpointOwnerUserName,
		config.EndpointName,
		config.AlgoOwnerUserName,
		config.AlgoName,
		algoIndex))

	for input, inputData := range inputMap {

		u, _ := url.Parse("localhost")

		u.Scheme = strings.ToLower(input.InputDeliveryType)
		if input.HttpPort > 0 {
			u.Host = fmt.Sprintf("localhost:%d", input.HttpPort)
		}
		u.Path = input.HttpPath

		q := u.Query()
		for _, param := range config.AlgoParams {
			q.Set(param.Name, param.Value)
		}
		u.RawQuery = q.Encode()

		for _, data := range inputData {
			request, reqErr := http.NewRequest(strings.ToLower(input.HttpVerb), u.String(), bytes.NewReader(data.data))
			if reqErr != nil {
				algoLog.Status = "Failed"
				algoLog.Log = fmt.Sprintf("Error building request: %s\n", reqErr)
				produceLogMessage(runID, logTopic, algoLog)
				continue
			}
			response, errReq := netClient.Do(request)

			reqDuration := time.Since(startTime)
			algoLog.RuntimeMs = int64(reqDuration / time.Millisecond)

			if errReq != nil {
				algoLog.Status = "Failed"
				algoLog.Log = fmt.Sprintf("Error getting response from http server: %s\n", errReq)
				produceLogMessage(runID, logTopic, algoLog)
				continue
			} else {
				defer response.Body.Close()
				// TODO: Get the content type and parse the contents
				// For example if multipart-form, get each file and load into kafka separately
				contents, errRead := ioutil.ReadAll(response.Body)
				if errRead != nil {
					algoLog.Status = "Failed"
					algoLog.Log = fmt.Sprintf("Error reading response from http server: %s\n", errRead)
					produceLogMessage(runID, logTopic, algoLog)
					continue
				}
				if response.StatusCode == 200 {
					// Send to output topic
					fileName, _ := uuid.NewV4()
					produceOutputMessage(runID, fileName.String(), outputTopic, contents)

					algoLog.Status = "Success"
					//algoLog.Log = string(contents)

					produceLogMessage(runID, logTopic, algoLog)

					return nil
				}

				// Produce the error to the log
				algoLog.Status = "Failed"
				algoLog.Log = fmt.Sprintf("Server returned non-success http status code: %d\n%s\n", response.StatusCode, contents)
				produceLogMessage(runID, logTopic, algoLog)

				return errors.New(algoLog.Log)

			}
		}

	}

	return errors.New(algoLog.Log)

}
