package main

import (
	"algo-runner-go/swagger"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	uuid "github.com/nu7hatch/gouuid"
)

func runHTTP(runID string,
	inputMap map[*swagger.AlgoInputModel][]InputData) (err error) {

	// startTime := time.Now()

	// Create the base message
	algoLog := logMessage{
		Type_:  "Algo",
		Status: "Started",
		Data: map[string]interface{}{
			"RunId":                 runID,
			"EndpointOwnerUserName": config.EndpointOwnerUserName,
			"EndpointName":          config.EndpointName,
			"AlgoOwnerUserName":     config.AlgoOwnerUserName,
			"AlgoName":              config.AlgoName,
			"AlgoVersionTag":        config.AlgoVersionTag,
			"AlgoIndex":             config.AlgoIndex,
			"AlgoInstanceName":      *instanceName,
		},
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
		config.AlgoIndex))

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
			request, reqErr := http.NewRequest(strings.ToUpper(input.HttpVerb), u.String(), bytes.NewReader(data.data))
			if reqErr != nil {
				algoLog.Status = "Failed"
				algoLog.Msg = fmt.Sprintf("Error building request: %s\n", reqErr)
				algoLog.log()
				continue
			}
			response, errReq := netClient.Do(request)

			// reqDuration := time.Since(startTime)
			// algoLog.AlgoLogData.RuntimeMs = int64(reqDuration / time.Millisecond)

			if errReq != nil {
				algoLog.Status = "Failed"
				algoLog.Msg = fmt.Sprintf("Error getting response from http server: %s\n", errReq)
				algoLog.log()
				continue
			} else {
				defer response.Body.Close()
				// TODO: Get the content type and parse the contents
				// For example if multipart-form, get each file and load into kafka separately
				contents, errRead := ioutil.ReadAll(response.Body)
				if errRead != nil {
					algoLog.Status = "Failed"
					algoLog.Msg = fmt.Sprintf("Error reading response from http server: %s\n", errRead)
					algoLog.log()
					continue
				}
				if response.StatusCode == 200 {
					// Send to output topic
					fileName, _ := uuid.NewV4()
					produceOutputMessage(fileName.String(), outputTopic, contents)

					algoLog.Status = "Success"
					algoLog.Msg = ""
					algoLog.log()

					return nil
				}

				// Produce the error to the log
				algoLog.Status = "Failed"
				algoLog.Msg = fmt.Sprintf("Server returned non-success http status code: %d\n%s\n", response.StatusCode, contents)
				algoLog.log()

				return errors.New(algoLog.Msg)

			}
		}

	}

	return errors.New(algoLog.Msg)

}
