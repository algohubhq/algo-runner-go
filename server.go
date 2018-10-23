package main

import (
	"algo-runner-go/swagger"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

func startServer() (terminated bool) {

	healthy = false

	// Create the base log message
	serverLog := logMessage{
		LogMessageType: "Server",
		Status:         "Started",
		ServerLogData: &swagger.ServerLogData{
			EndpointOwnerUserName: config.EndpointOwnerUserName,
			EndpointName:          config.EndpointName,
			AlgoOwnerUserName:     config.AlgoOwnerUserName,
			AlgoName:              config.AlgoName,
			AlgoVersionTag:        config.AlgoVersionTag,
			AlgoInstanceName:      *instanceName,
		},
	}

	terminated = false

	serverCmd := strings.Split(config.Entrypoint, " ")

	cmd := exec.Command(serverCmd[0], serverCmd[1:]...)

	// setup termination on kill signals
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigchan

		serverLog.Status = "Terminated"
		serverLog.ServerLogData.Log = fmt.Sprintf("Caught signal %v. Killing server process: %s\n", sig, config.Entrypoint)
		serverLog.log()

		if cmd != nil && cmd.Process != nil {
			val := cmd.Process.Kill()
			terminated = true
			if val != nil {
				serverLog.Status = "Terminated"
				serverLog.ServerLogData.Log = fmt.Sprintf("Killed server process: %s - error %s\n", config.Entrypoint, val.Error())
				serverLog.log()
			}
		}
	}()

	var stdout, stderr []byte
	var errStdout, errStderr error
	stdoutIn, _ := cmd.StdoutPipe()
	stderrIn, _ := cmd.StderrPipe()
	err := cmd.Start()

	if err != nil {
		serverLog.Status = "Failed"
		serverLog.ServerLogData.Log = fmt.Sprintf("Server start failed for command '%s' with error '%s'\n", config.Entrypoint, err)
		serverLog.log()
	} else {
		serverLog.Status = "Running"
		serverLog.ServerLogData.Log = fmt.Sprintf("Server started with command '%s'\n", config.Entrypoint)
		serverLog.log()
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		stdout, errStdout = captureOutput(serverLog, os.Stdout, stdoutIn)
		wg.Done()
	}()

	go func() {
		stderr, errStderr = captureOutput(serverLog, os.Stderr, stderrIn)
		wg.Done()
	}()

	wg.Wait()

	errWait := cmd.Wait()
	if errWait != nil {
		serverLog.Status = "Failed"
		serverLog.ServerLogData.Log = fmt.Sprintf("Server start failed with %s\n", errWait)
		serverLog.log()
	}
	if errStdout != nil || errStderr != nil {
		serverLog.Status = "Failed"
		serverLog.ServerLogData.Log = fmt.Sprintf("Failed to capture stdout or stderr for the server process.\n")
		serverLog.log()
	}

	// If this is reached, the server has terminated (bad)
	terminated = true
	outBytes := append(stderr, stdout...)

	serverLog.Status = "Terminated"
	serverLog.ServerLogData.Log = fmt.Sprintf("Server Terminated unexpectedly!\n%s\n", string(outBytes))
	serverLog.log()

	return

}

func captureOutput(serverLog logMessage, w io.Writer, r io.Reader) ([]byte, error) {

	var out []byte
	buf := make([]byte, 1024, 1024)
	for {
		n, err := r.Read(buf[:])
		if n > 0 {
			d := buf[:n]
			out = append(out, d...)

			// deliver at 100K blocks for large messages
			if len(out) >= 102400 {
				if len(out) > 0 {
					serverLog.Status = "Running"
					serverLog.ServerLogData.Log = string(out)
					serverLog.log()
				}

				out = nil
			}

			_, err := w.Write(d)
			if err != nil {

				if len(out) > 0 {
					serverLog.Status = "Running"
					serverLog.ServerLogData.Log = string(out)
					serverLog.log()
				}

				return out, err
			}
		}
		if err != nil {
			// Read returns io.EOF at the end of file, which is not an error for us
			if err == io.EOF {
				err = nil
			}

			if len(out) > 0 {
				serverLog.Status = "Running"
				serverLog.ServerLogData.Log = string(out)
				serverLog.log()
			}

			return out, err
		}
	}

	// never reached

}
