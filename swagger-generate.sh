#!/bin/bash

# Local instance must be running to pull the swagger.json file
java -jar ./swagger-codegen-cli.jar generate -i https://localhost:5443/swagger/v1/swagger.json -Dio.swagger.parser.util.RemoteUrl.trustAll=true -l go -o algorun-go-client

mkdir -p ./swagger/
cp ./algorun-go-client/runner_config.go ./swagger/
cp ./algorun-go-client/algo_param_model.go ./swagger/
cp ./algorun-go-client/topic_config_model.go ./swagger/
cp ./algorun-go-client/topic_param_model.go ./swagger/
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
cp ./algorun-go-client/log_entry_model.go ./swagger/

rm -rf ./algorun-go-client/

