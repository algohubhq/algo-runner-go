package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	config := loadConfig("./config.json")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "172.22.238.110:32768",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	for _, route := range config.PipelineRoutes {

		switch routeType := route.RouteType; routeType {
		case "Algo":
			fmt.Println("Route Type Algo")
		case "DataSource":
			fmt.Println("Route Type DataSource")
		case "EndpointSource":
			fmt.Println("Route Type EndpointSource")
		}

	}

	c.SubscribeTopics([]string{"myTopic", "^aRegex.*[Tt]opic"}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}

	c.Close()
}
