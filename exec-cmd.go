package main

import (
	"os/exec"
)

// New creates a new ExecCmd.
func (execRunner *ExecRunner) newExecCmd() *exec.Cmd {

	execCmd := exec.Command(execRunner.command[0], execRunner.command[1:]...)
	return execCmd

}
