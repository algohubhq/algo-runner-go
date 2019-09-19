package main

import (
	"algo-runner-go/swagger"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"

	uuid "github.com/nu7hatch/gouuid"
)

// ExecCmd is the executable Algo
type ExecCmd struct {
	targetCmd  *exec.Cmd
	sendStdout bool
}

// New creates a new ExecCmd.
func newExecCmd() *ExecCmd {

	command := getCommand(config)

	targetCmd := exec.Command(command[0], command[1:]...)

	envs := getEnvironment(config)
	if len(envs) > 0 {
		//targetCmd.Env = envs
	}

	algoName := fmt.Sprintf("%s/%s:%s[%d]", config.AlgoOwnerUserName, config.AlgoName, config.AlgoVersionTag, config.AlgoIndex)

	var sendStdout bool
	// Set the arguments for the output
	for _, output := range config.Outputs {

		handleOutput := config.WriteAllOutputs
		outputMessageDataType := "Embedded"

		// Check to see if there are any mapped routes for this output and get the message data type
		for i := range config.Pipes {
			if config.Pipes[i].SourceName == algoName {
				handleOutput = true
				outputMessageDataType = config.Pipes[i].SourceOutputMessageDataType
				break
			}
		}

		if handleOutput {

			switch outputDeliveryType := output.OutputDeliveryType; strings.ToLower(outputDeliveryType) {
			case "fileparameter":

				fileUUID, _ := uuid.NewV4()
				fileID := strings.Replace(fileUUID.String(), "-", "", -1)

				folder := path.Join("/data",
					config.EndpointOwnerUserName,
					config.EndpointName,
					config.AlgoOwnerUserName,
					config.AlgoName,
					string(config.AlgoIndex),
					output.Name)

				fileFolder := path.Join(folder, fileID)
				if _, err := os.Stat(folder); os.IsNotExist(err) {
					os.MkdirAll(folder, os.ModePerm)
				}
				// Set the output parameter
				if output.Parameter != "" {
					targetCmd.Args = append(targetCmd.Args, output.Parameter)
					targetCmd.Args = append(targetCmd.Args, fileFolder)
				}

				// Watch for a specific file.
				// TODO: start a mc exec command
				// outputWatcher.watch(fileFolder, config.AlgoIndex, &output, outputMessageDataType)

			case "folderparameter":
				// Watch folder for changes.

				folder := path.Join("/data",
					config.EndpointOwnerUserName,
					config.EndpointName,
					config.AlgoOwnerUserName,
					config.AlgoName,
					string(config.AlgoIndex),
					output.Name)

				if _, err := os.Stat(folder); os.IsNotExist(err) {
					os.MkdirAll(folder, os.ModePerm)
				}
				// Set the output parameter
				if output.Parameter != "" {
					targetCmd.Args = append(targetCmd.Args, output.Parameter)
					targetCmd.Args = append(targetCmd.Args, folder)
				}

				// Watch for a specific file.
				// TODO: start a mc exec command
				// outputWatcher.watch(folder, config.AlgoIndex, &output, outputMessageDataType)

			case "stdout":
				sendStdout = true

			}

		}
	}

	return &ExecCmd{
		targetCmd:  targetCmd,
		sendStdout: sendStdout,
	}

}

func getCommand(config swagger.AlgoRunnerConfig) []string {

	cmd := strings.Split(config.Entrypoint, " ")

	for _, param := range config.AlgoParams {
		cmd = append(cmd, param.Name)
		if param.DataType.Name != "switch" {
			cmd = append(cmd, param.Value)
		}
	}

	return cmd
}

func getEnvironment(config swagger.AlgoRunnerConfig) []string {

	env := strings.Split(config.Entrypoint, " ")

	return env
}
