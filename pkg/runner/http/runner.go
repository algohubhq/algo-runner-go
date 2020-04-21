package httprunner

import (
	kafkaproducer "algo-runner-go/pkg/kafka/producer"
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/metrics"
	"algo-runner-go/pkg/openapi"
	"algo-runner-go/pkg/storage"
	"algo-runner-go/pkg/types"
	"bytes"
	"encoding/binary"
	"encoding/json"
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
	Config        *openapi.AlgoRunnerConfig
	Logger        *logging.Logger
	Metrics       *metrics.Metrics
	Producer      *kafkaproducer.Producer
	StorageConfig *storage.Storage
	InstanceName  string
}

// New creates a new Http Runner.
func NewHTTPRunner(config *openapi.AlgoRunnerConfig,
	producer *kafkaproducer.Producer,
	storageConfig *storage.Storage,
	instanceName string,
	logger *logging.Logger,
	metrics *metrics.Metrics) *HTTPRunner {

	return &HTTPRunner{
		Config:        config,
		Producer:      producer,
		StorageConfig: storageConfig,
		InstanceName:  instanceName,
		Logger:        logger,
		Metrics:       metrics,
	}

}

func (r *HTTPRunner) Run(key string,
	traceID string,
	endpointParams string,
	inputMap map[*openapi.AlgoInputSpec][]types.InputData) (err error) {

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

	algoName := fmt.Sprintf("%s/%s:%s[%d]", r.Config.Owner, r.Config.Name, r.Config.Version, r.Config.Index)

	for input, inputData := range inputMap {

		var outputTopic string
		var output openapi.AlgoOutputSpec
		// get the httpresponse output
		for _, o := range r.Config.Outputs {
			if *o.OutputDeliveryType == openapi.OUTPUTDELIVERYTYPES_HTTP_RESPONSE &&
				strings.ToLower(o.Name) == strings.ToLower(input.Name) {
				output = o
				outputTopic = strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.%d.output.%s",
					r.Config.DeploymentOwner,
					r.Config.DeploymentName,
					r.Config.Owner,
					r.Config.Name,
					r.Config.Index,
					o.Name))
			}
		}

		outputMessageDataType := openapi.MESSAGEDATATYPES_EMBEDDED

		// Check to see if there are any mapped routes for this output and get the message data type
		for i := range r.Config.Pipes {
			if r.Config.Pipes[i].SourceName == algoName {
				outputMessageDataType = *r.Config.Pipes[i].SourceOutputMessageDataType
				break
			}
		}

		u, _ := url.Parse("localhost")
		u.Scheme = strings.ToLower(string(*input.InputDeliveryType))
		if input.HttpPort > 0 {
			u.Host = fmt.Sprintf("localhost:%d", input.HttpPort)
		}
		u.Path = input.HttpPath

		// Include the endpoint params as querystring parameters
		endpointQuery, err := url.ParseQuery(endpointParams)
		if err != nil {
			r.Logger.Error(fmt.Sprintf("Error parsing endpoint parameters from query string"), err)
			continue
		}

		q := u.Query()
		for _, param := range r.Config.Parameters {
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
			request, reqErr := http.NewRequest(strings.ToUpper(input.HttpVerb), u.String(), bytes.NewReader(data.Data))
			if data.ContentType != "" {
				request.Header.Set("Content-Type", data.ContentType)
			}
			if reqErr != nil {
				r.Logger.Error("Error building http request.", reqErr)
				continue
			}
			response, errReq := netClient.Do(request)

			if errReq != nil {
				r.Logger.Error("Error getting response from http server.", errReq)

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
					r.Logger.Error("Error reading response from http server", errRead)
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
					// If outputmessage data type is embedded, then send the contents in message
					if outputMessageDataType == openapi.MESSAGEDATATYPES_EMBEDDED {
						// Send to output topic
						fileName, _ := uuid.NewV4()

						if outputTopic != "" {
							r.Metrics.DataBytesOutputCounter.WithLabelValues(r.Metrics.DeploymentLabel,
								r.Metrics.PipelineLabel,
								r.Metrics.ComponentLabel,
								r.Metrics.AlgoLabel,
								r.Metrics.AlgoVersionLabel,
								r.Metrics.AlgoIndexLabel,
								output.Name,
								"ok").Add(float64(binary.Size(contents)))

							r.Producer.ProduceOutputMessage(key, traceID, fileName.String(), outputTopic, output.Name, contents)

						} else {
							r.Logger.Error("No output topic with outputDeliveryType as HttpResponse for input that is an http request",
								errors.New("No output topic"))
							return nil
						}
					} else {

						// Upload the data to storage and send the file reference to Kafka
						// Try to create the json
						fileName, _ := uuid.NewV4()
						bucketName := fmt.Sprintf("%s.%s",
							strings.ToLower(r.Config.DeploymentOwner),
							strings.ToLower(r.Config.DeploymentName))
						fileReference := openapi.FileReference{
							Host:   r.StorageConfig.Host,
							Bucket: bucketName,
							File:   fileName.String(),
						}
						jsonBytes, jsonErr := json.Marshal(fileReference)
						if jsonErr != nil {
							r.Logger.Error("Unable to create the file reference json.", jsonErr)
						}

						err = r.StorageConfig.Uploader.Upload(fileReference, contents)
						if err != nil {
							// Create error message
							r.Logger.Error(fmt.Sprintf("Error uploading to storage for file reference [%s]", fileReference.File), err)
						}

						r.Metrics.DataBytesOutputCounter.WithLabelValues(r.Metrics.DeploymentLabel,
							r.Metrics.PipelineLabel,
							r.Metrics.ComponentLabel,
							r.Metrics.AlgoLabel,
							r.Metrics.AlgoVersionLabel,
							r.Metrics.AlgoIndexLabel,
							output.Name,
							"ok").Add(float64(binary.Size(contents)))

						r.Producer.ProduceOutputMessage(key, traceID, fileName.String(), outputTopic, output.Name, jsonBytes)

					}

					return nil
				}

				// Produce the error to the log
				r.Logger.Error(fmt.Sprintf("Server returned non-success http status code: %d", response.StatusCode), errors.New(string(contents)))

				return errors.New(r.Logger.LogMessage.Msg)

			}
		}

	}

	return errors.New(r.Logger.LogMessage.Msg)

}
