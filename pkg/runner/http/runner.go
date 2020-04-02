package httprunner

import (
	kafkaproducer "algo-runner-go/pkg/kafka/producer"
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	"algo-runner-go/pkg/types"
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

// HTTPRunner holds the configuration for the the http runner
type HTTPRunner struct {
	types.IRunner
	Config       *openapi.AlgoRunnerConfig
	Logger       *logging.Logger
	Metrics      *metrics.Metrics
	Producer     *kafkaproducer.Producer
	InstanceName string
}

// New creates a new Http Runner.
func NewHTTPRunner(config *openapi.AlgoRunnerConfig,
	producer *kafkaproducer.Producer,
	storageConfig *types.StorageConfig,
	instanceName string,
	logger *logging.Logger,
	metrics *metrics.Metrics) *HTTPRunner {

	return &HTTPRunner{
		Config:       config,
		Producer:     producer,
		InstanceName: instanceName,
		Logger:       logger,
		Metrics:      metrics,
	}

}

func (r *HTTPRunner) Run(traceID string,
	endpointParams string,
	inputMap map[*openapi.AlgoInputModel][]types.InputData) (err error) {

	// TODO: Write to the topic as error if no value
	if inputMap == nil {

		// 	return
	}

	var netClient = &http.Client{
		Timeout: time.Second * 10,
	}

	// Set the timeout
	if r.Config.TimeoutSeconds > 0 {
		netClient.Timeout = time.Second * time.Duration(r.Config.TimeoutSeconds)
	}

	for input, inputData := range inputMap {

		var outputTopic string
		// get the httpresponse output
		for _, output := range r.Config.Outputs {
			if output.OutputDeliveryType == openapi.OUTPUTDELIVERYTYPES_HTTP_RESPONSE &&
				strings.ToLower(output.Name) == strings.ToLower(input.Name) {
				outputTopic = strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.%d.output.%s",
					r.Config.DeploymentOwnerUserName,
					r.Config.DeploymentName,
					r.Config.AlgoOwnerUserName,
					r.Config.AlgoName,
					r.Config.AlgoIndex,
					output.Name))
			}
		}

		u, _ := url.Parse("localhost")
		u.Scheme = strings.ToLower(string(input.InputDeliveryType))
		if input.HttpPort > 0 {
			u.Host = fmt.Sprintf("localhost:%d", input.HttpPort)
		}
		u.Path = *input.HttpPath

		// Include the endpoint params as querystring parameters
		endpointQuery, err := url.ParseQuery(endpointParams)
		if err != nil {
			r.Logger.LogMessage.Msg = fmt.Sprintf("Error parsing endpoint parameters from query string")
			r.Logger.Log(err)
			continue
		}

		q := u.Query()
		for _, param := range r.Config.AlgoParams {
			q.Set(param.Name, param.Value)
			// overwrite any parameters passed from the endpoint
			for endpointParam, endpointValSlice := range endpointQuery {
				if strings.ToLower(param.Name) == strings.ToLower(endpointParam) {
					for _, endpointVal := range endpointValSlice {
						q.Set(endpointParam, endpointVal)
					}
				}
			}
		}

		u.RawQuery = q.Encode()

		for _, data := range inputData {

			startTime := time.Now()
			request, reqErr := http.NewRequest(strings.ToUpper(*input.HttpVerb), u.String(), bytes.NewReader(data.Data))
			if data.ContentType != "" {
				request.Header.Set("Content-Type", data.ContentType)
			}
			if reqErr != nil {
				r.Logger.LogMessage.Msg = fmt.Sprintf("Error building request")
				r.Logger.Log(reqErr)
				continue
			}
			response, errReq := netClient.Do(request)

			if errReq != nil {
				r.Logger.LogMessage.Msg = fmt.Sprintf("Error getting response from http server.")
				r.Logger.Log(errReq)

				reqDuration := time.Since(startTime)
				r.Metrics.AlgoRuntimeHistogram.WithLabelValues(r.Metrics.DeploymentLabel,
					r.Metrics.PipelineLabel,
					r.Metrics.ComponentLabel,
					r.Metrics.AlgoLabel,
					r.Metrics.AlgoVersionLabel,
					r.Metrics.AlgoIndexLabel).Observe(reqDuration.Seconds())

				continue

			} else {
				defer response.Body.Close()
				// TODO: Get the content type and parse the contents
				// For example if multipart-form, get each file and load into kafka separately
				contents, errRead := ioutil.ReadAll(response.Body)
				if errRead != nil {
					r.Logger.LogMessage.Msg = fmt.Sprintf("Error reading response from http server")
					r.Logger.Log(errRead)
					continue
				}

				reqDuration := time.Since(startTime)
				r.Metrics.AlgoRuntimeHistogram.WithLabelValues(r.Metrics.DeploymentLabel,
					r.Metrics.PipelineLabel,
					r.Metrics.ComponentLabel,
					r.Metrics.AlgoLabel,
					r.Metrics.AlgoVersionLabel,
					r.Metrics.AlgoIndexLabel).Observe(reqDuration.Seconds())

				if response.StatusCode == 200 {
					// Send to output topic
					fileName, _ := uuid.NewV4()

					if outputTopic != "" {
						r.Producer.ProduceOutputMessage(traceID, fileName.String(), outputTopic, contents)
					} else {
						r.Logger.LogMessage.Msg = fmt.Sprintf("No output topic with outputDeliveryType as HttpResponse for input that is an http request")
						r.Logger.Log(nil)
						return nil
					}

					return nil
				}

				// Produce the error to the log
				r.Logger.LogMessage.Msg = fmt.Sprintf("Server returned non-success http status code: %d", response.StatusCode)
				r.Logger.Log(errors.New(string(contents)))

				return errors.New(r.Logger.LogMessage.Msg)

			}
		}

	}

	return errors.New(r.Logger.LogMessage.Msg)

}
