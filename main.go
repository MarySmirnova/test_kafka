package main

import (
	"fmt"
	"kafka/kafka"
	"log"
	"time"

	kafkaGO "github.com/segmentio/kafka-go"
)

const (
	broker = "localhost:9092"
	topic  = "test-topic"
	group  = "test-consumer-group"
)

func main() {
	kfk, err := kafka.New([]string{broker}, topic, group)
	if err != nil {
		panic(err)
	}

	timeout := 1 * time.Second

	errChan := make(chan error)
	msgChan := make(chan string)

	go sendTimeToKafkaWithTimeout(kfk, timeout, errChan)

	go readMessageFromKafka(kfk, msgChan, errChan)

	go func() {
		err := <-errChan
		log.Fatal(err)
	}()

	for msg := range msgChan {
		fmt.Println(msg)
	}
}

func sendTimeToKafkaWithTimeout(kfk *kafka.Client, timeout time.Duration, errChan chan<- error) {
	for {
		err := kfk.SendMessages([]kafkaGO.Message{
			{
				Value: []byte(time.Now().String()),
			},
		})
		if err != nil {
			errChan <- err
			break
		}

		<-time.After(timeout)
	}
}

func readMessageFromKafka(kfk *kafka.Client, msgChan chan<- string, errChan chan<- error) {
	for {
		msg, err := kfk.GetMessage()
		if err != nil {
			errChan <- err
			break
		}

		msgChan <- string(msg.Value)

		if err := kfk.CommitMessage(msg); err != nil {
			errChan <- err
			break
		}
	}
}
