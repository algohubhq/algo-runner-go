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

	// Create the base message
	algoLog := logMessage{
		Type_:   "Algo",
		Status:  "Started",
		Version: "1",
		Data: map[string]interface{}{
			"RunId":                   runID,
			"DeploymentOwnerUserName": config.DeploymentOwnerUserName,
			"DeploymentName":          config.DeploymentName,
			"AlgoOwnerUserName":       config.AlgoOwnerUserName,
			"AlgoName":                config.AlgoName,
			"AlgoVersionTag":          config.AlgoVersionTag,
			"AlgoIndex":               config.AlgoIndex,
			"AlgoInstanceName":        *instanceName,
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
		config.DeploymentOwnerUserName,
		config.DeploymentName,
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

			startTime := time.Now()
			request, reqErr := http.NewRequest(strings.ToUpper(input.HttpVerb), u.String(), bytes.NewReader(data.data))
			if reqErr != nil {
				algoLog.Status = "Failed"
				algoLog.Msg = fmt.Sprintf("Error building request")
				algoLog.log(reqErr)
				continue
			}
			response, errReq := netClient.Do(request)

			if errReq != nil {
				algoLog.Status = "Failed"
				algoLog.Msg = fmt.Sprintf("Error getting response from http server.")
				algoLog.log(errReq)

				reqDuration := time.Since(startTime)
				algoRuntimeHistogram.WithLabelValues(deploymentLabel, algoLabel, algoLog.Status).Observe(reqDuration.Seconds())

				continue

			} else {
				defer response.Body.Close()
				// TODO: Get the content type and parse the contents
				// For example if multipart-form, get each file and load into kafka separately
				contents, errRead := ioutil.ReadAll(response.Body)
				if errRead != nil {
					algoLog.Status = "Failed"
					algoLog.Msg = fmt.Sprintf("Error reading response from http server")
					algoLog.log(errRead)
					continue
				}

				reqDuration := time.Since(startTime)
				algoRuntimeHistogram.WithLabelValues(deploymentLabel, algoLabel, algoLog.Status).Observe(reqDuration.Seconds())

				if response.StatusCode == 200 {
					// Send to output topic
					fileName, _ := uuid.NewV4()
					produceOutputMessage(fileName.String(), outputTopic, contents)

					algoLog.Status = "Success"
					algoLog.Msg = ""
					algoLog.log(nil)

					return nil
				}

				// Produce the error to the log
				algoLog.Status = "Failed"
				algoLog.Msg = fmt.Sprintf("Server returned non-success http status code: %d", response.StatusCode)
				algoLog.log(errors.New(string(contents)))

				return errors.New(algoLog.Msg)

			}
		}

	}

	return errors.New(algoLog.Msg)

}
