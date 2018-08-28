package main

import (
	"algo-runner-go/swagger"
	"encoding/json"
	"fmt"
	"github.com/radovskyb/watcher"
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"
)

// OutputWatcher describes a process that watches for new files created by an output.
type OutputWatcher struct {
	fileWatcher *watcher.Watcher
	algoIndex   int32
	outputs     map[string]*output
}

type output struct {
	outputMessageDataType string
	algoOutput            *swagger.AlgoOutputModel
}

// New creates a new Output Watcher.
func newOutputWatcher() *OutputWatcher {

	// Setup all output file watchers
	fileWatcher := watcher.New()
	fileWatcher.SetMaxEvents(1)
	fileWatcher.FilterOps(watcher.Create)

	return &OutputWatcher{
		fileWatcher: fileWatcher,
		outputs:     make(map[string]*output),
		algoIndex:   0,
	}

}

func (outputWatcher *OutputWatcher) watch(fileFolder string, algoIndex int32, algoOutput *swagger.AlgoOutputModel, outputMessageDataType string) (err error) {

	if err := outputWatcher.fileWatcher.AddRecursive(fileFolder); err != nil {
		return err
	}
	outputWatcher.algoIndex = algoIndex
	outputWatcher.outputs[fileFolder] = &output{algoOutput: algoOutput, outputMessageDataType: outputMessageDataType}

	return nil

}

func (outputWatcher *OutputWatcher) start() {

	// Create the base log message
	runnerLog := logMessage{
		LogMessageType:        "Runner",
		EndpointOwnerUserName: config.EndpointOwnerUserName,
		EndpointName:          config.EndpointName,
		AlgoOwnerUserName:     config.AlgoOwnerUserName,
		AlgoName:              config.AlgoName,
		AlgoVersionTag:        config.AlgoVersionTag,
		Status:                "Running",
	}

	// go routine to handle file changes
	go func() {

		for {
			select {
			case event := <-outputWatcher.fileWatcher.Event:

				fileName := event.Name()

				var output *output
				var algoOutput *swagger.AlgoOutputModel
				dir := filepath.Dir(event.Path)
				// Check to match the output by the filename
				if matchedOutput, ok := outputWatcher.outputs[event.Path]; ok {
					// the output matched the full file name
					output = matchedOutput
					algoOutput = matchedOutput.algoOutput
				} else {
					// split the path from the filename to get the output associated with the path
					if matchedOutput, ok := outputWatcher.outputs[dir]; ok {
						output = matchedOutput
						algoOutput = matchedOutput.algoOutput
					}
				}

				fileOutputTopic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.%d.output.%s",
					config.EndpointOwnerUserName,
					config.EndpointName,
					config.AlgoOwnerUserName,
					config.AlgoName,
					outputWatcher.algoIndex,
					algoOutput.Name))

				// Write to output topic
				// Check if the output should be embedded or file reference
				// TODO: Test if you can watch a file in a network path
				if output.outputMessageDataType == "FileReference" {

					// Try to create the json
					fileReference := FileReference{FilePath: dir, FileName: fileName}
					jsonBytes, jsonErr := json.Marshal(fileReference)

					if jsonErr != nil {
						runnerLog.log("Failed", fmt.Sprintf("Unable to create the file reference json with error: %s\n", jsonErr))
					}

					produceOutputMessage(fileName, fileOutputTopic, jsonBytes)

				} else {

					fileBytes, err := ioutil.ReadFile(event.Path)
					if err != nil {
						runnerLog.log("Failed", fmt.Sprintf("Output watcher unable to read the file [%s] from disk with error: %s\n", event.Path, err))
					}

					produceOutputMessage(fileName, fileOutputTopic, fileBytes)

				}

			case err := <-outputWatcher.fileWatcher.Error:
				runnerLog.log("Failed", fmt.Sprintf("Output watcher error watching output file/folder: %s/n", err))
			case <-outputWatcher.fileWatcher.Closed:
				return
			}
		}
	}()

	go func() {
		if err := outputWatcher.fileWatcher.Start(time.Millisecond * 10); err != nil {
			runnerLog.log("Failed", fmt.Sprintf("Output watcher failed to start with error: %s/n", err))
		}
	}()

}

func (outputWatcher *OutputWatcher) closeOutputWatcher() {

	if outputWatcher.fileWatcher != nil {
		outputWatcher.fileWatcher.Close()
	}

	if outputWatcher.outputs != nil {
		outputWatcher.outputs = nil
	}

}