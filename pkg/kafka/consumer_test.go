package kafka

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// mockConsumerGroupSession is a mock implementation of sarama.ConsumerGroupSession
type mockConsumerGroupSession struct {
	ctx context.Context
}

func (m *mockConsumerGroupSession) Claims() map[string][]int32 {
	return make(map[string][]int32)
}

func (m *mockConsumerGroupSession) MemberID() string {
	return "test-member"
}

func (m *mockConsumerGroupSession) GenerationID() int32 {
	return 1
}

func (m *mockConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	// Do nothing
}

func (m *mockConsumerGroupSession) Commit() {
	// Do nothing
}

func (m *mockConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	// Do nothing
}

func (m *mockConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	// Do nothing
}

func (m *mockConsumerGroupSession) Context() context.Context {
	return m.ctx
}

// mockConsumerGroupClaim is a mock implementation of sarama.ConsumerGroupClaim
type mockConsumerGroupClaim struct {
	topic               string
	partition           int32
	messages            chan *sarama.ConsumerMessage
	initial             int64
	highWaterMarkOffset int64
}

func (m *mockConsumerGroupClaim) Topic() string {
	return m.topic
}

func (m *mockConsumerGroupClaim) Partition() int32 {
	return m.partition
}

func (m *mockConsumerGroupClaim) InitialOffset() int64 {
	return m.initial
}

func (m *mockConsumerGroupClaim) HighWaterMarkOffset() int64 {
	return m.highWaterMarkOffset
}

func (m *mockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return m.messages
}

func TestNewConsumer(t *testing.T) {
	// Create a test consumer config
	config := ConsumerConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: "test-group",
		Topics:  []string{"test-topic"},
		Logger:  zerolog.New(os.Stdout).With().Timestamp().Str("component", "test-consumer").Logger(),
		SaramaConfig: func() *sarama.Config {
			saramaConfig := sarama.NewConfig()
			saramaConfig.Consumer.Return.Errors = true
			return saramaConfig
		}(),
	}

	// Skip the actual test if we're not in a test environment
	t.Skip("Skipping test that requires Kafka broker")

	// Create a new consumer
	consumer, err := NewConsumer(config)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	// Close the consumer
	err = consumer.Close()
	assert.NoError(t, err)
}

func TestSetMessageHandler(t *testing.T) {
	// Create a consumer
	consumer := &Consumer{
		topics: []string{"test-topic"},
		logger: zerolog.New(os.Stdout).With().Timestamp().Str("component", "test-consumer").Logger(),
	}

	// Set a message handler
	handler := func(msg *sarama.ConsumerMessage) error {
		return nil
	}
	consumer.SetMessageHandler(handler)

	// Verify the handler was set
	assert.NotNil(t, consumer.handler)
}

func TestConsumerGroupHandlerConsumeClaim(t *testing.T) {
	// Create a logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Str("component", "test-consumer").Logger()

	// Create a message handler
	var receivedMessage *sarama.ConsumerMessage
	handler := func(msg *sarama.ConsumerMessage) error {
		receivedMessage = msg
		return nil
	}

	// Create a consumer group handler
	consumerHandler := consumerGroupHandler{
		logger:  logger,
		handler: handler,
	}

	// Create a mock session and claim
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &mockConsumerGroupSession{
		ctx: ctx,
	}

	messages := make(chan *sarama.ConsumerMessage, 1)
	claim := &mockConsumerGroupClaim{
		topic:     "test-topic",
		partition: 0,
		messages:  messages,
	}

	// Send a test message
	testMessage := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    0,
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
	}

	// Start consuming in a goroutine
	go func() {
		// Send the message after a short delay
		time.Sleep(100 * time.Millisecond)
		messages <- testMessage

		// Cancel the context after another delay to stop the consumer
		time.Sleep(100 * time.Millisecond)
		cancel()
		close(messages)
	}()

	// Consume the message
	err := consumerHandler.ConsumeClaim(session, claim)
	assert.NoError(t, err)

	// Verify the message was received
	assert.Equal(t, testMessage, receivedMessage)
}

func TestConsumerStartWithNoHandler(t *testing.T) {
	// Create a consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer := &Consumer{
		topics: []string{"test-topic"},
		logger: zerolog.New(os.Stdout).With().Timestamp().Str("component", "test-consumer").Logger(),
		ctx:    ctx,
		cancel: cancel,
	}

	// Try to start without a handler
	err := consumer.Start(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "no message handler defined", err.Error())
}

func TestConsumerHandlerError(t *testing.T) {
	// Create a logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Str("component", "test-consumer").Logger()

	// Set a message handler that returns an error
	expectedError := errors.New("test error")
	handler := func(msg *sarama.ConsumerMessage) error {
		return expectedError
	}

	// Create a consumer group handler
	consumerHandler := consumerGroupHandler{
		logger:  logger,
		handler: handler,
	}

	// Create a mock session and claim
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &mockConsumerGroupSession{
		ctx: ctx,
	}

	messages := make(chan *sarama.ConsumerMessage, 1)
	claim := &mockConsumerGroupClaim{
		topic:     "test-topic",
		partition: 0,
		messages:  messages,
	}

	// Send a test message
	testMessage := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    0,
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
	}

	// Start consuming in a goroutine
	go func() {
		// Send the message after a short delay
		time.Sleep(100 * time.Millisecond)
		messages <- testMessage

		// Cancel the context after another delay to stop the consumer
		time.Sleep(100 * time.Millisecond)
		cancel()
		close(messages)
	}()

	// Consume the message
	err := consumerHandler.ConsumeClaim(session, claim)
	assert.NoError(t, err)
}
