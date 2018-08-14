package main

import (
	"algo-runner-go/swagger"
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
	}

}

func (outputWatcher *OutputWatcher) watch(fileFolder string, algoOutput *swagger.AlgoOutputModel, outputMessageDataType string) (err error) {

	if err := outputWatcher.fileWatcher.AddRecursive(fileFolder); err != nil {
		return err
	}

	outputWatcher.outputs[fileFolder] = &output{algoOutput: algoOutput, outputMessageDataType: outputMessageDataType}

	return nil

}

func (outputWatcher *OutputWatcher) start() {

	// go routine to handle file changes
	go func() {
		for {
			select {
			case event := <-outputWatcher.fileWatcher.Event:

				var algoOutput *swagger.AlgoOutputModel
				// Check to match the output by the filename
				if output, ok := outputWatcher.outputs[event.Path]; ok {
					// the output matched the full file name
					algoOutput = output.algoOutput
				} else {
					// split the path from the filename to get the output associated with the path
					dir := filepath.Dir(event.Path)
					if output, ok := outputWatcher.outputs[dir]; ok {
						algoOutput = output.algoOutput
					}
				}

				outputName := strings.Replace(algoOutput.Name, " ", "", -1)

				fileOutputTopic := strings.ToLower(fmt.Sprintf("algorun.%s.%s.algo.%s.%s.output.%s",
					config.EndpointOwnerUserName,
					config.EndpointUrlName,
					config.AlgoOwnerUserName,
					config.AlgoUrlName,
					outputName))

				fmt.Println(fileOutputTopic)

				// Write to output topic
				// TODO: Check if the output should be embedded or file reference
				// outputMessageDataType

				fileName := event.Name()

				fileBytes, err := ioutil.ReadFile(event.Path)
				if err != nil {
					fmt.Println(err.Error())
				}

				produceOutputMessage(runID, fileName, fileOutputTopic, fileBytes)

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
