package main

import (
	"algo-runner-go/swagger"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
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
	serverLog := swagger.ServerLogModel{
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
		stdout, errStdout = logToOrchestrator("stdout", serverLog, kafkaServers, os.Stdout, stdoutIn)
		wg.Done()
	}()

	go func() {
		serverLog.Status = "Failed"
		stderr, errStderr = logToOrchestrator("stderr", serverLog, kafkaServers, os.Stderr, stderrIn)
		wg.Done()
	}()

	wg.Wait()

	errWait := cmd.Wait()
	if err != nil {
		serverLog.Status = "Failed"
		serverLog.StdErr = fmt.Sprintf("Server start failed with %s\n", errWait)
		produceMessage(serverLog, kafkaServers)
	}
	if errStdout != nil || errStderr != nil {
		fmt.Fprintf(os.Stderr, "failed to capture stdout or stderr\n")
	}

	// If this is reached, the server has terminated (bad)
	terminated = true
	outStr, errStr := string(stdout), string(stderr)
	serverLog.Status = "Terminated"
	serverLog.StdOut = outStr
	serverLog.StdErr = errStr
	produceMessage(serverLog, kafkaServers)

	fmt.Fprintf(os.Stderr, "Server Terminated unexpectedly!\n")

	return
}

func logToOrchestrator(logType string, serverLog swagger.ServerLogModel, kafkaServers *string, w io.Writer, r io.Reader) ([]byte, error) {

	var out []byte
	buf := make([]byte, 1024, 1024)
	for {
		n, err := r.Read(buf[:])
		if n > 0 {
			d := buf[:n]
			out = append(out, d...)

			// deliver at 100K blocks for large messages
			if len(out) >= 102400 {
				if logType == "stdout" && len(out) > 0 {
					serverLog.StdOut = string(out)
					produceMessage(serverLog, kafkaServers)
				}
				if logType == "stderr" && len(out) > 0 {
					serverLog.StdErr = string(out)
					produceMessage(serverLog, kafkaServers)
				}

				out = nil
			}

			_, err := w.Write(d)
			if err != nil {

				if logType == "stdout" && len(out) > 0 {
					serverLog.StdOut = string(out)
					produceMessage(serverLog, kafkaServers)
				}
				if logType == "stderr" && len(out) > 0 {
					serverLog.StdErr = string(out)
					produceMessage(serverLog, kafkaServers)
				}

				return out, err
			}
		}
		if err != nil {
			// Read returns io.EOF at the end of file, which is not an error for us
			if err == io.EOF {
				err = nil
			}

			if logType == "stdout" && len(out) > 0 {
				serverLog.StdOut = string(out)
				produceMessage(serverLog, kafkaServers)
			}
			if logType == "stderr" && len(out) > 0 {
				serverLog.StdErr = string(out)
				produceMessage(serverLog, kafkaServers)
			}

			return out, err
		}
	}

	// never reached

}

func produceMessage(serverLog swagger.ServerLogModel, kafkaServers *string) {

	topic := "algorun.orchestrator.servers"

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": *kafkaServers})

	if err != nil {
		fmt.Printf("Failed to create server message producer: %s\n", err)
		os.Exit(1)
	}

	doneChan := make(chan bool)

	go func() {
		defer close(doneChan)
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				return

			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	serverLogBytes, err := json.Marshal(serverLog)

	p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value: serverLogBytes}

	// wait for delivery report goroutine to finish
	_ = <-doneChan

	p.Close()

}
