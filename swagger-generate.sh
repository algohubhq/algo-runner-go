#!/bin/bash

# Local instance must be running to pull the swagger.json file
java -jar ./swagger-codegen-cli.jar generate -i http://localhost:5000/swagger/v1/swagger.json -l go -o algorun-go-client

mkdir ./swagger/
cp ./algorun-go-client/runner_config.go ./swagger/
cp ./algorun-go-client/algo_param_model.go ./swagger/
cp ./algorun-go-client/data_type_model.go ./swagger/
cp ./algorun-go-client/data_type_option_model.go ./swagger/
cp ./algorun-go-client/media_type_model.go ./swagger/
cp ./algorun-go-client/algo_input_model.go ./swagger/
cp ./algorun-go-client/algo_output_model.go ./swagger/
cp ./algorun-go-client/pipe_model.go ./swagger/
cp ./algorun-go-client/pipeline_data_connector_model.go ./swagger/
cp ./algorun-go-client/data_connector_model.go ./swagger/
cp ./algorun-go-client/data_connector_option_model.go ./swagger/
cp ./algorun-go-client/algo_config.go ./swagger/
cp ./algorun-go-client/log_message.go ./swagger/
cp ./algorun-go-client/runner_log_data.go ./swagger/
cp ./algorun-go-client/server_log_data.go ./swagger/
cp ./algorun-go-client/algo_log_data.go ./swagger/
cp ./algorun-go-client/orchestrator_log_data.go ./swagger/
cp ./algorun-go-client/endpoint_log_data.go ./swagger/
cp ./algorun-go-client/deployment_log_data.go ./swagger/
cp ./algorun-go-client/instance_log_data.go ./swagger/

rm -rf ./algorun-go-client/

