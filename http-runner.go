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
	inputMap map[*swagger.AlgoInputModel][]InputData) (err error) {

	startTime := time.Now()

	// Create the base message
	algoLog := swagger.LogMessage{
		LogMessageType:        "Algo",
		EndpointOwnerUserName: config.EndpointOwnerUserName,
		EndpointUrlName:       config.EndpointUrlName,
		AlgoOwnerUserName:     config.AlgoOwnerUserName,
		AlgoUrlName:           config.AlgoUrlName,
		AlgoVersionTag:        config.AlgoVersionTag,
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

	outputTopic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.output.default",
		config.EndpointOwnerUserName,
		config.EndpointUrlName,
		config.AlgoOwnerUserName,
		config.AlgoUrlName))

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
					reqDuration := time.Since(startTime)
					fileName, _ := uuid.NewV4()
					produceOutputMessage(runID, fileName.String(), outputTopic, contents)

					algoLog.Status = "Success"
					algoLog.RuntimeMs = int64(reqDuration / time.Millisecond)
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
