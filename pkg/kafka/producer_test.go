package kafka

import (
	"os"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestNewProducer(t *testing.T) {
	// Create a test producer config
	config := ProducerConfig{
		Brokers: []string{"localhost:9092"},
		Logger:  zerolog.New(os.Stdout).With().Timestamp().Str("component", "test-producer").Logger(),
		// Use a custom SaramaConfig that will create a mock producer
		SaramaConfig: func() *sarama.Config {
			saramaConfig := sarama.NewConfig()
			saramaConfig.Producer.Return.Successes = true
			saramaConfig.Producer.Return.Errors = true
			return saramaConfig
		}(),
	}

	// Skip the actual test if we're not in a test environment
	// In a real test environment, you would use a mock Kafka broker
	t.Skip("Skipping test that requires Kafka broker")

	// Create a new producer
	producer, err := NewProducer(config)
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	// Close the producer
	err = producer.Close()
	assert.NoError(t, err)
}

func TestSendMessage(t *testing.T) {
	// Create a mock producer
	mockProducer := mocks.NewAsyncProducer(t, nil)

	// Set expectations for message send
	mockProducer.ExpectInputAndSucceed()

	// Create a producer with the mock
	producer := &Producer{
		producer: mockProducer,
		logger:   zerolog.New(os.Stdout).With().Timestamp().Str("component", "test-producer").Logger(),
	}

	// Test sending a message
	err := producer.SendMessage("test-topic", []byte("test-key"), []byte("test-value"), nil)
	assert.NoError(t, err)

	// Close the producer - this will also close the mock producer
	err = producer.Close()
	assert.NoError(t, err)
}

func TestSendMessageWithNilProducer(t *testing.T) {
	// Create a producer with nil producer
	producer := &Producer{
		producer: nil,
		logger:   zerolog.New(os.Stdout).With().Timestamp().Str("component", "test-producer").Logger(),
	}

	// Test sending a message with nil producer
	err := producer.SendMessage("test-topic", []byte("test-key"), []byte("test-value"), nil)
	assert.Error(t, err)
	assert.Equal(t, "producer is not initialized", err.Error())

	// Close the producer
	err = producer.Close()
	assert.NoError(t, err)
}
