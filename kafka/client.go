package kafka

import (
	"context"
	"errors"

	"github.com/segmentio/kafka-go"
)

type FileMessage struct {
	Type string
	Path string
	Name string
}

type Client struct {
	Writer *kafka.Writer
	Reader *kafka.Reader
}

func New(brokers []string, topic string, groupId string) (*Client, error) {
	if len(brokers) == 0 || brokers[0] == "" || topic == "" || groupId == "" {
		return nil, errors.New("не указаны параметры подключения к Kafka")
	}

	c := &Client{}

	c.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupId,
		MinBytes: 10e1,
		MaxBytes: 10e6,
	})

	c.Writer = &kafka.Writer{
		Addr:     kafka.TCP(brokers[0]),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	return c, nil
}

func (c *Client) SendMessages(messages []kafka.Message) error {
	return c.Writer.WriteMessages(context.Background(), messages...)
}

func (c *Client) GetMessage() (kafka.Message, error) {
	return c.Reader.FetchMessage(context.Background())
}

func (c *Client) CommitMessage(msg kafka.Message) error {
	return c.Reader.CommitMessages(context.Background(), msg)
}
