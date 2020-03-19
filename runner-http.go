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

func runHTTP(runID string, endpointParams string,
	inputMap map[*swagger.AlgoInputModel][]InputData) (err error) {

	// Create the base message
	runnerLog := logMessage{
		Type_:   "Runner",
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

	for input, inputData := range inputMap {

		var outputTopic string
		// get the httpresponse output
		for _, output := range config.Outputs {
			if strings.ToLower(output.OutputDeliveryType) == "httpresponse" &&
				strings.ToLower(output.Name) == strings.ToLower(input.Name) {
				outputTopic = strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.%d.output.%s",
					config.DeploymentOwnerUserName,
					config.DeploymentName,
					config.AlgoOwnerUserName,
					config.AlgoName,
					config.AlgoIndex,
					output.Name))
			}
		}

		u, _ := url.Parse("localhost")
		// Include the endpoint params as querystring parameters
		u.RawQuery = endpointParams

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
			if data.contentType != "" {
				request.Header.Set("Content-Type", data.contentType)
			}
			if reqErr != nil {
				runnerLog.Status = "Failed"
				runnerLog.Msg = fmt.Sprintf("Error building request")
				runnerLog.log(reqErr)
				continue
			}
			response, errReq := netClient.Do(request)

			if errReq != nil {
				runnerLog.Status = "Failed"
				runnerLog.Msg = fmt.Sprintf("Error getting response from http server.")
				runnerLog.log(errReq)

				reqDuration := time.Since(startTime)
				algoRuntimeHistogram.WithLabelValues(deploymentLabel, componentLabel, algoLabel, algoVersionLabel, algoIndexLabel).Observe(reqDuration.Seconds())

				continue

			} else {
				defer response.Body.Close()
				// TODO: Get the content type and parse the contents
				// For example if multipart-form, get each file and load into kafka separately
				contents, errRead := ioutil.ReadAll(response.Body)
				if errRead != nil {
					runnerLog.Status = "Failed"
					runnerLog.Msg = fmt.Sprintf("Error reading response from http server")
					runnerLog.log(errRead)
					continue
				}

				reqDuration := time.Since(startTime)
				algoRuntimeHistogram.WithLabelValues(deploymentLabel, componentLabel, algoLabel, algoVersionLabel, algoIndexLabel).Observe(reqDuration.Seconds())

				if response.StatusCode == 200 {
					// Send to output topic
					fileName, _ := uuid.NewV4()

					if outputTopic != "" {
						produceOutputMessage(fileName.String(), outputTopic, contents)
					} else {
						runnerLog.Status = "Failed"
						runnerLog.Msg = fmt.Sprintf("No output topic with outputDeliveryType as HttpResponse for input that is an http request")
						runnerLog.log(nil)
						return nil
					}

					return nil
				}

				// Produce the error to the log
				runnerLog.Status = "Failed"
				runnerLog.Msg = fmt.Sprintf("Server returned non-success http status code: %d", response.StatusCode)
				runnerLog.log(errors.New(string(contents)))

				return errors.New(runnerLog.Msg)

			}
		}

	}

	return errors.New(runnerLog.Msg)

}
