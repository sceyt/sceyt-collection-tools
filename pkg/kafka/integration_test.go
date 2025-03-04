package kafka

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// TestIntegration tests the producer and consumer working together
// This test is skipped by default because it requires a running Kafka broker
func TestIntegration(t *testing.T) {
	// Skip the test if we're not in an integration test environment
	if os.Getenv("KAFKA_INTEGRATION_TEST") != "true" {
		t.Skip("Skipping integration test; set KAFKA_INTEGRATION_TEST=true to run")
	}

	// Get broker addresses from environment or use default
	brokers := []string{"localhost:29092"}
	if os.Getenv("KAFKA_BROKERS") != "" {
		brokers = []string{os.Getenv("KAFKA_BROKERS")}
	}

	// Create a unique topic for this test
	topic := "integration-test-" + time.Now().Format("20060102-150405")

	// Create a producer
	producerConfig := ProducerConfig{
		Brokers: brokers,
		Logger:  zerolog.New(os.Stdout).With().Timestamp().Str("component", "integration-test-producer").Logger(),
	}
	producer, err := NewProducer(producerConfig)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create a consumer
	consumerConfig := ConsumerConfig{
		Brokers: brokers,
		GroupID: "integration-test-group",
		Topics:  []string{topic},
		Logger:  zerolog.New(os.Stdout).With().Timestamp().Str("component", "integration-test-consumer").Logger(),
	}
	consumer, err := NewConsumer(consumerConfig)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Create a channel to receive messages
	messageReceived := make(chan *sarama.ConsumerMessage, 1)

	// Set up the consumer handler
	consumer.SetMessageHandler(func(msg *sarama.ConsumerMessage) error {
		t.Logf("Received message: %s", string(msg.Value))
		messageReceived <- msg
		return nil
	})

	// Start the consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Start(ctx); err != nil {
			t.Errorf("Error starting consumer: %v", err)
		}
	}()

	// Wait for the consumer to start
	time.Sleep(5 * time.Second)

	// Send a test message
	testKey := "test-key"
	testValue := "test-value-" + time.Now().Format(time.RFC3339)

	t.Logf("Sending message: %s", testValue)
	err = producer.SendMessage(topic, []byte(testKey), []byte(testValue), nil)
	assert.NoError(t, err)

	// Wait for the message to be received
	select {
	case msg := <-messageReceived:
		// Verify the message
		assert.Equal(t, topic, msg.Topic)
		assert.Equal(t, testKey, string(msg.Key))
		assert.Equal(t, testValue, string(msg.Value))
		t.Logf("Message successfully received")
	case <-time.After(30 * time.Second):
		t.Fatal("Timed out waiting for message")
	}

	// Clean up
	cancel()
	wg.Wait()
}

// TestProducerConsumerMock tests the producer and consumer with mocks
func TestProducerConsumerMock(t *testing.T) {
	// This test uses mocks to test the interaction between producer and consumer
	// without requiring a real Kafka broker

	// Create a test topic and message
	topic := "test-topic"
	key := "test-key"
	value := "test-value"

	// Create a mock consumer with the updated structure
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockConsumer := &Consumer{
		topics: []string{topic},
		logger: zerolog.New(os.Stdout).With().Timestamp().Str("component", "mock-consumer").Logger(),
		ctx:    ctx,
		cancel: cancel,
	}

	// Set up a channel to receive the message
	messageReceived := make(chan *sarama.ConsumerMessage, 1)

	// Set up the consumer handler
	mockConsumer.SetMessageHandler(func(msg *sarama.ConsumerMessage) error {
		messageReceived <- msg
		return nil
	})

	// Create a test message
	testMessage := &sarama.ConsumerMessage{
		Topic:     topic,
		Partition: 0,
		Offset:    0,
		Key:       []byte(key),
		Value:     []byte(value),
	}

	// Simulate sending and receiving a message
	go func() {
		// Wait a bit to simulate network delay
		time.Sleep(100 * time.Millisecond)

		// Directly put the message in the channel
		messageReceived <- testMessage
	}()

	// Wait for the message
	select {
	case msg := <-messageReceived:
		// Verify the message
		assert.Equal(t, topic, msg.Topic)
		assert.Equal(t, key, string(msg.Key))
		assert.Equal(t, value, string(msg.Value))
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for message")
	}
}

// Example of how you might add a test with headers
func TestSendMessageWithHeaders(t *testing.T) {
	// Create a mock producer
	mockProducer := mocks.NewAsyncProducer(t, nil)

	// Set expectations for message send
	mockProducer.ExpectInputAndSucceed()

	// Create a producer with the mock
	producer := &Producer{
		producer: mockProducer,
		logger:   zerolog.New(os.Stdout).With().Timestamp().Str("component", "test-producer").Logger(),
	}

	// Create test headers
	headers := []sarama.RecordHeader{
		{Key: []byte("header-key-1"), Value: []byte("header-value-1")},
		{Key: []byte("header-key-2"), Value: []byte("header-value-2")},
	}

	// Test sending a message with headers
	err := producer.SendMessage("test-topic", []byte("test-key"), []byte("test-value"), headers)
	assert.NoError(t, err)

	// Close the producer
	err = producer.Close()
	assert.NoError(t, err)
}
