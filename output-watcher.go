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

				fmt.Println(fileOutputTopic)

				// Write to output topic
				// Check if the output should be embedded or file reference
				// TODO: Test if you can watch a file in a network path
				if output.outputMessageDataType == "FileReference" {

					// Try to read the json
					fileReference := FileReference{FilePath: dir, FileName: fileName}
					jsonBytes, jsonErr := json.Marshal(fileReference)

					if jsonErr != nil {
						fmt.Println(jsonErr.Error())
					}

					produceOutputMessage(runID, fileName, fileOutputTopic, jsonBytes)

				} else {

					fileBytes, err := ioutil.ReadFile(event.Path)
					if err != nil {
						fmt.Println(err.Error())
					}

					produceOutputMessage(runID, fileName, fileOutputTopic, fileBytes)

				}

			case err := <-outputWatcher.fileWatcher.Error:
				fmt.Printf("Error watching output file/folder: %s/n", err)
			case <-outputWatcher.fileWatcher.Closed:
				return
			}
		}
	}()

	go func() {
		if err := outputWatcher.fileWatcher.Start(time.Millisecond * 10); err != nil {
			// TODO: Log the error
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
