package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("mensagem2", "teste", producer, nil, deliveryChan)
	go DeliveryReport(deliveryChan) //async
	fmt.Println("Printa antes da chamada assincrona")
	producer.Flush(9000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "host.docker.internal:9094",
	}
	p, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Println(err.Error())
	}
	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliverychan chan kafka.Event){
	for e:= range deliverychan{
		switch ev:= e.(type){
		case *kafka.Message:
			if ev.TopicPartition.Error != nil{
				fmt.Println("Erro ao enviar")
			}else{
				fmt.Println("Mensagem enviada", ev.TopicPartition)
			}
		}
	}
}