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

func startServer(config swagger.RunnerConfig, kafkaServers *string) (terminated bool) {

	terminated = false

	serverCmd := strings.Split(config.Entrypoint, " ")

	cmd := exec.Command(serverCmd[0], serverCmd[1:]...)

	// setup termination on kill signals
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigchan
		fmt.Printf("Caught signal %v. Killing server process: %s\n", sig, config.Entrypoint)
		if cmd != nil && cmd.Process != nil {
			val := cmd.Process.Kill()
			terminated = true
			if val != nil {
				fmt.Printf("Killed server process: %s - error %s\n", config.Entrypoint, val.Error())
			}
		}
	}()

	var stdout, stderr []byte
	var errStdout, errStderr error
	stdoutIn, _ := cmd.StdoutPipe()
	stderrIn, _ := cmd.StderrPipe()
	err := cmd.Start()

	if err != nil {
		fmt.Fprintf(os.Stderr, "Server cmd.Start() failed with '%s'\n", err)
	}

	// Create the base message
	serverLog := swagger.LogMessage{
		LogMessageType:        "Server",
		EndpointOwnerUserName: config.EndpointOwnerUserName,
		EndpointUrlName:       config.EndpointUrlName,
		AlgoOwnerUserName:     config.AlgoOwnerUserName,
		AlgoUrlName:           config.AlgoUrlName,
		AlgoVersionTag:        config.AlgoVersionTag,
		Status:                "Started",
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		serverLog.LogSource = "stdout"
		stdout, errStdout = logToOrchestrator(serverLog, kafkaServers, os.Stdout, stdoutIn)
		wg.Done()
	}()

	go func() {
		serverLog.LogSource = "stderr"
		serverLog.Status = "Failed"
		stderr, errStderr = logToOrchestrator(serverLog, kafkaServers, os.Stderr, stderrIn)
		wg.Done()
	}()

	wg.Wait()

	errWait := cmd.Wait()
	if err != nil {
		serverLog.LogSource = "stderr"
		serverLog.Status = "Failed"
		serverLog.Log = fmt.Sprintf("Server start failed with %s\n", errWait)
		produceLogMessage(getLogTopic(), kafkaServers, serverLog)
	}
	if errStdout != nil || errStderr != nil {
		fmt.Fprintf(os.Stderr, "failed to capture stdout or stderr\n")
	}

	// If this is reached, the server has terminated (bad)
	terminated = true
	outBytes := append(stderr, stdout...)

	serverLog.Status = "Terminated"
	serverLog.Log = string(outBytes)

	produceLogMessage(getLogTopic(), kafkaServers, serverLog)

	fmt.Fprintf(os.Stderr, "Server Terminated unexpectedly!\n")

	return
}

func logToOrchestrator(serverLog swagger.LogMessage, kafkaServers *string, w io.Writer, r io.Reader) ([]byte, error) {

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
					serverLog.Log = string(out)
					produceLogMessage(getLogTopic(), kafkaServers, serverLog)
				}

				out = nil
			}

			_, err := w.Write(d)
			if err != nil {

				if len(out) > 0 {
					serverLog.Log = string(out)
					produceLogMessage(getLogTopic(), kafkaServers, serverLog)
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
				serverLog.Log = string(out)
				produceLogMessage(getLogTopic(), kafkaServers, serverLog)
			}

			return out, err
		}
	}

	// never reached

}

func getLogTopic() string {
	return "algorun.orchestrator.logs"
}
