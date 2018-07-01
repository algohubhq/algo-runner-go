package main

import (
	"flag"
	"os"
	"strings"
)

func main() {

	configFilePtr := flag.String("config", "./config.json", "JSON config file to load")
	kafkaServersPtr := flag.String("kafka-servers", "localhost:9092", "Kafka broker addresses separated by a comma")

	flag.Parse()

	config := loadConfig(*configFilePtr)

	// Launch the server if not started
	if strings.ToLower(config.ServerType) != "serverless" {

		var serverTerminated bool
		go func() {
			serverTerminated = startServer(config, *kafkaServersPtr)
			if serverTerminated {
				os.Exit(1)
			}
		}()

	}

	startConsumer(config, *kafkaServersPtr)

}
