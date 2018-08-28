package main

import (
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

	// Create the base message
	serverLog := logMessage{
		LogMessageType:        "Server",
		EndpointOwnerUserName: config.EndpointOwnerUserName,
		EndpointName:          config.EndpointName,
		AlgoOwnerUserName:     config.AlgoOwnerUserName,
		AlgoName:              config.AlgoName,
		AlgoVersionTag:        config.AlgoVersionTag,
		Status:                "Started",
	}

	terminated = false

	serverCmd := strings.Split(config.Entrypoint, " ")

	cmd := exec.Command(serverCmd[0], serverCmd[1:]...)

	// setup termination on kill signals
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigchan

		serverLog.log("Terminated", fmt.Sprintf("Caught signal %v. Killing server process: %s\n", sig, config.Entrypoint))

		if cmd != nil && cmd.Process != nil {
			val := cmd.Process.Kill()
			terminated = true
			if val != nil {
				serverLog.log("Terminated", fmt.Sprintf("Killed server process: %s - error %s\n", config.Entrypoint, val.Error()))
			}
		}
	}()

	var stdout, stderr []byte
	var errStdout, errStderr error
	stdoutIn, _ := cmd.StdoutPipe()
	stderrIn, _ := cmd.StderrPipe()
	err := cmd.Start()

	if err != nil {
		serverLog.log("Failed", fmt.Sprintf("Server start failed for command '%s' with error '%s'\n", config.Entrypoint, err))
	} else {
		serverLog.log("Running", fmt.Sprintf("Server started with command '%s'\n", config.Entrypoint))
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
	if err != nil {
		serverLog.log("Failed", fmt.Sprintf("Server start failed with %s\n", errWait))
	}
	if errStdout != nil || errStderr != nil {
		serverLog.log("Failed", fmt.Sprintf("Failed to capture stdout or stderr for the server process.\n"))
	}

	// If this is reached, the server has terminated (bad)
	terminated = true
	outBytes := append(stderr, stdout...)

	serverLog.log("Terminated", fmt.Sprintf("Server Terminated unexpectedly!\n%s\n", string(outBytes)))

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
					serverLog.log("Running", string(out))
				}

				out = nil
			}

			_, err := w.Write(d)
			if err != nil {

				if len(out) > 0 {
					serverLog.log("Running", string(out))
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
				serverLog.log("Running", string(out))
			}

			return out, err
		}
	}

	// never reached

}
