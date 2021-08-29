package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "host.docker.internal:9094",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
	}

	c, err := kafka.NewConsumer(configMap)

	if err != nil {
		log.Println("erro consumer", err.Error())
	}

	topics := []string{"teste"}

	c.SubscribeTopics(topics, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		}
		fmt.Println(string(msg.Value))
	}
}
