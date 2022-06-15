package kafka

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

const (
	broker = "localhost:9092"
	topic  = "test-topic"
	group  = "test-consumer-group"
)

var messages = []kafka.Message{
	{
		Key:   []byte("Test Key"),
		Value: []byte("Test Value"),
	},
}

func initClient(t *testing.T) *Client {
	kfk, err := New(
		[]string{broker},
		topic,
		group,
	)
	assert.Nil(t, err)

	return kfk
}

func TestClient_sendMessages(t *testing.T) {
	kfk := initClient(t)

	err := kfk.SendMessages(messages)
	assert.Nil(t, err)
}

func TestClient_getMessage_and_commit(t *testing.T) {
	kfk := initClient(t)

	msg, err := kfk.GetMessage()
	assert.Nil(t, err)

	err = kfk.CommitMessage(msg)
	assert.Nil(t, err)
}
