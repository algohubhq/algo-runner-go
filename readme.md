## Introduction

AlgoRunner is a proxy component that is attached to each Algo as a kubernetes sidecar container, typically added by the Algo.Run Pipeline Operator. The AlgoRunner agent subscribes to the upstream Kafka topics required to fulfill the Algo inputs. This separates the boilerplate event consuming logic from the actual Algo implementation and relieves Algo developers from having to implement the necessary features required to run a reliable event pipeline. Instead you can focus on the Algo code and leave the data management, routing, performance metrics, error handling and retries to the AlgoRunner agent.

This is intended to be used in conjunction with the [Algo.Run Pipeline Operator](https://github.com/algohubhq/pipeline-operator).

The AlgoRunner agent adds many features to your existing services:

## Features

* **Consumer** - Automatic configuration of Kafka Consumers for all pipes routed to inputs if its associated Algo.
* **[TLS](#tls-configuration)** - Configuration of TLS certificates for secure broker, consumer and producer communication.
* **[Authentication](#authentication)** - Configuration of TLS client auth, scram-sha-512 or plain authentication.
* **Execution** - Runs HTTP, gRPC or executables with the ability to relay parameter values through Kafka and transform them into querystring parameters or arguments (for executables) and collects the resulting output.
* **[Delegation](#delegated-mode)** - If the built-in kafka consumer / producer does not meet your needs, the AlgoRunner can be set to delegate control to the Algo implementation.
* **Output Producer** - The result from the HTTP, gRPC or executable is captured and output data is produced to Kafka. The output data can either be embedded in the Kafka message or written to S3 compatible storage and the file reference sent in the Kafka message.
* **[Failure Retry](#error-handling)** - Implements flexible error handling, retry logic and dead letter queues through simple configurations.
* **[Metrics](#metrics)** - Prometheus metrics are exposed by the AlgoRunner for performance monitoring, troubleshooting and benchmarking Algo implementations.

## Workflow

![Algo Deployment](https://content.algohub.com/assets/Algo-Deployment.jpg)

When an Algo is deployed with the Algo.Run Pipeline Operator, a Pod is created with two containers. One container is the Algo implementation and the second is the AlgoRunner sidecar container. The deployment is mounted with file system volumes to hold any input and output data. How the Algo is deployed is described in more detail in the Deployment Overview.

Once the Algo pod is deployed and ready, it will begin to process any data that is routed to it through a virtual 'pipe' configured within the pipeline. The AlgoRunner manages the processing through a series of stages:

- A data record is read from Kafka for all inputs to the Algo.
- If a transformer is configured, a data transformation is applied to the record in preparation for the Algo processing.
- The input data is delivered according to the needs of the Algo implementation.
- If the Algo accepts StdIn, HTTP or gRPC input, the data is never written to disk and it is relayed directly to the input.
- If the Algo requires file input, the record(s) are written to the file system and the file path(s) are delivered to the Algo as parameters.
- The Algo output(s) are monitored and the data is produced into the Kafka stream to be piped to any additional pipeline stages.
- If the output is written to StdOut, a HTTP or gRPC response, the output data is not written to disk and it is produced directly to Kafka.
- If the output is written to a file or folder, there are two potential ways to configure the output. The file can either be embedded and produced to Kafka or the files are mirrored to shared storage and a file reference record is produced into Kafka. For more information on output handling see below.


## TLS Configuration

TLS for the Kafka consumer is configured through environment variables. When AlgoRunner is deployed from the Pipeline Operator, these values are automatically configured and the certs mounted.

The environment variables that control the TLS configuration are:
- KAFKA_TLS - Set to 'true' to enable TLS.
- KAFKA_TLS_CA_LOCATION - The path within the container for the Kafka broker certificate authority cert in X.509 format. The default is '/etc/ssl/certs/kafka-ca.crt'

## Authentication

Kafka Authentication is also configured through environment variables. Similar to TLS, when AlgoRunner is deployed from the Pipeline Operator, these values are automatically configured and the certs mounted.

The environment variables that control Kafka authentication configuration are:
- KAFKA_AUTH_TYPE - Options are tls, scram-sha-512 or plain.
- KAFKA_AUTH_TLS_USER_LOCATION - Used when KAFKA_AUTH_TYPE is tls, this is the path within the container for the Kafka user certificate in X.509 format. The default is '/etc/ssl/certs/kafka-user.crt'.
- KAFKA_AUTH_TLS_KEY_LOCATION - Used when KAFKA_AUTH_TYPE is tls, this is the path within the container for the Kafka user private key. The default is '/etc/ssl/certs/kafka-user.key'.
- KAFKA_AUTH_USERNAME - Used when KAFKA_AUTH_TYPE is scram-sha-512 or plain, this is the Kafka username.
- KAFKA_AUTH_PASSWORD - Used when KAFKA_AUTH_TYPE is scram-sha-512 or plain, this is the Kafka user password. 

## Delegated Mode

Delegated mode disables the Kafka Consumer and Producer in the AlgoRunner to allow the Algo implementation to retain control over the process of consuming and producing Kafka messages. This is useful is situations where you the HTTP, gRPC or executable methods will not work for your needs.
This mode still enables the usage of your Algo in any arbitrary pipeline by providing your implementation with environment variables needed to configure your consumer / producer.

The Algo.Run Pipeline operator configures Kafka topics and sets the topic names for outputs when a pipeline is deployed. Because these topic names are dynamic and generated for each deployment, the topic names are sent to the delegated algo as environment variables.

The environment variables provided are:
- KAFKA_BROKERS - The list of Kafka brokers to be used for the Consumer / Producer
- KAFKA_INPUT_TOPIC_{{INPUT-NAME}} - An env var for each input of the Algo. The value is the topic name containing the data for the associated input.
- KAFKA_OUTPUT_TOPIC_{{OUTPUT-NAME}} - An env var for each output of the Algo. The value is the topic name to produce data to for the associated output.

## Error Handling

Errors are handled by the AlgoRunner based on the executor type.
- If the executor is HTTP, when the HTTP call returns a non-success response code (not HTTP 20X)
- If the executor is gRPC, when the gRPC call returns a non-success response code (not OK 1)
- If the executor is Executable, the stderr is monitored and if anything is output to stderr, it is considered an error state.

If an error is detected, the response is logged as an error and the retry strategy is executed.

## Retry Strategy

A retry strategy can be configured to allow failed executions to be attempted again after a configurable backoff time period. Currently there are two retry strategies available with two additional strategies in development. 

- Simple - The 'Simple' strategy will retry the execution after waiting the fixed backoff duration by putting the consumer to sleep. This strategy is best if you require messages to be processed in order. The major downside is subsequent messages are also delayed until the retry strategy for the failed message has fully executed.
- Retry Topics - The 'RetryTopics' strategy will create a set of additional topics, one topic for each backoff duration. By separating each retry into separate topics, the consumers are non-blocking and allow additional messages to continue to be processed.

The retry strategy has the following configuration:
- Steps - An array of retry steps. Each step has the settings:
  - Index - The order of the step.
  - BackoffDuration - The length of time to wait before retrying. The value is formatted as a Golang duration string. (ie 30s, 5m, 1h, 1d) [Golang Duration docs](https://golang.org/pkg/time/#Duration).
  - Repeat - Number of times to repeat this step.
- DeadLetterSuffix - The suffix for the topic name of the Dead Letter Queue.

Once all retry steps have been exhausted, the message is added to the Dead Letter Queue for manual troubleshooting and processing.

Additional retry strategies are under development, StatefulOrdered and StatefulUnordered, which use an external persistent storage to maintain the state of the failed messages and ordering.

## Metrics

Prometheus metrics are exposed by the AlgoRunner to enable monitoring of the Algo implementation.  By default the metrics are exposed on the container at http://<<ip/host>>:10080/metrics. The metrics list can be found in the source code at [Metrics](https://github.com/algohubhq/algo-runner-go/tree/master/pkg/metrics/metrics.go)

## Configuration Options

Here is a [sample config](https://github.com/algohubhq/algo-runner-go/tree/master/configs/test.json) for reference.

Below is some of the core metadata that is required to inform the AlgoRunner how to manage and run the Algo implementation.

#### Algo Owner

Every Algo is scoped to the username or organization that owns the Algo. Similar to the way a git repository is scoped to it's owner in github.com. If adding an Algo to a local AlgoRun instance that is not connected to your AlgoHub.com account, the Algo will be scoped to a reserved name called 'local'.

#### Algo Name

The Algo name is a url friendly shortened name. The name must must all lowercase consisting of alphanumeric characters or single hyphens. It cannot begin or end in a hyphen and has a maximum or 50 characters.

#### Algo Title

The Algo Title is a human friendly name with a maximum of 256 characters.

#### Versions

Each Algo definition can contain many versions of the Algo implementation. Each version consists of:
- Version Tag - Each version must be tagged with a semantic version or any arbitrary version tagging method. The version tag does not need to match the version of the code itself but for clarity, it probably should.
- Docker image repository - The url for the docker image repository. This can the AlgoHub.com registry, a public docker registry like hub.docker.com or quay.io, or it can be a private docker registry. 
- Docker image tag - The docker image tag to use for this version
- Entrypoint - The executable entrypoint for this version. While this likely stays the same with version changes, sometimes it is required to adjust the startup commands for future versions. If no entrypoint is defined, the entrypoint or command defined in the dockerfile will be used.

#### Timeout

The default execution timeout, in Seconds, should be set for the Algo. The actual timeout that is used for Algo execution is determined in the following order:
1. Pipeline defined timeout - A pipeline can override the timeout for each usage of the Algo.
2. Algo defined timeout - The default Algo timeout as defined by the author.
3. Global default timeout - A global execution timeout defined in the AlgoRun configuration.


#### Parameters

Parameters are a list of arguments that will be set for the Algo. If the Algo is an executable or script, the parameters are translated into command line arguments and flags. If the Algo is behind a HTTP server, the parameters are sent in the querystring.
The more complete the parameter metadata is, the pipeline designer can better render the UI for configuring an Algo. Each parameter has the following metadata:
- Name - the actual name / key for the command line flag or querystring variable that will be sent.
- Description - a human friendly description of the parameter and how it is used.
- Value - the default value for the parameter. This value will be used when running the Algo if it is not overridden in the pipeline configuration.
- Data Type - the data type for the value of this parameter. By clearly defining the data type, a UI can be constructed in AlgoRun to effectively display possible configuration values and ensure the data is validated.
- Options - if the data type is a Select list of enumerated values, the available options can be defined here.


#### Inputs

Every Algo must have one or more input. An input defines how the Algo will accept data to be processed. Each input must be configured with the following:

- Name - A short, url friendly name for the input. The name must must all lowercase consisting of alphanumeric characters or single hyphens. It cannot begin or end in a hyphen and has a maximum or 50 characters.
- Description - A human friendly description of the input and how it is used.
- Input delivery type - The input delivery type defines how incoming data will be passed to the Algo. The following options are available:

	- StdIn - Input data will be piped into the Standard Input of the executable or script. The Algo must be designed to read from StdIn for this option to be used.
	- HTTP(S) - Input data will delivered to the HTTP server using the configured verb (POST, PUT, GET), port, header and path.
	- gRPC - Input data will be serialized into the defined Protobuf schema.
	- Parameter - Input data will be written to the file system and the file path sent as a command line argument.
	- Repeated Parameter - Input data will be written as multiple files to the file system and each file will be delivered as command line arguments that repeat the same parameter name.
	- Delimited Parameter - Input data will be written as multiple files to the file system and each file will be delivered as a list of files names delimited by the configured delimiter as a single command line argument.
	- Environment Variable - Input data will be written to an environment variable.
- Content Types - The accepted content types can be defined. By defining the content type for an input you gain additional features:
	- Ensure only compatible outputs and inputs can be piped to each other
	- Validation of the data being delivered to ensure it matches the content type


#### Outputs

Every Algo must also have one or more output. An output defined how the Algo delivers the results of it's processing.
- Name - A short, url friendly name for the output. The name must must all lowercase consisting of alphanumeric characters or single hyphens. It cannot begin or end in a hyphen and has a maximum or 50 characters.
- Description - A human friendly description of the output and how it is used.
- Output Delivery Type - The output delivery type defines how the result data is written. The following options are available:

	- StdOut - Output results are written directly to StdOut. This can be useful for simple Algos that do not require logging, as StdOut cannot be used for logging with this option. All StdOut lines will be delivered into the Kafka output topic.
	- File Parameter - Output results are written to a specific file. This file will be watched by the AlgoRunner and if a write is detected, the file will be captured and delivered into the Kafka output topic.
	- Folder Parameter - Output results are written to a folder. All files written to this folder will be watched by the AlgoRunner and if a write is detected, the file will be captured and delivered into the Kafka output topic.
	- Http Response - When the Algo is a HTTP server, the HTTP response will be delivered into the Kafka output topic.
- Content Type - The content type for the data generated by this output. By defining the content type for an output you gain additional features:
	- Ensure only compatible outputs and inputs can be piped to each other
