package main

import (
	"algo-runner-go/swagger"
	"encoding/json"
	"fmt"
	"io/ioutil"
)

func loadConfig(fileName string) swagger.RunnerConfig {

	raw, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Println(err.Error())
	}

	var c swagger.RunnerConfig
	jsonErr := json.Unmarshal(raw, &c)

	if jsonErr != nil {
		fmt.Println(jsonErr.Error())
	}

	return c

}
