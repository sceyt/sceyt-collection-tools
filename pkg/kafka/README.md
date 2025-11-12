# Kafka Package

This package provides a simple, robust implementation for interacting with Apache Kafka using the Sarama library. It includes both producer and consumer components with configurable options and error handling.

## Components

### Consumer

The consumer component provides functionality to consume messages from Kafka topics using consumer groups. It handles:

- Connection to Kafka brokers
- Consumer group management
- Message processing with custom handlers
- Error handling and recovery
- Offset management

### Producer

The producer component provides functionality to produce messages to Kafka topics. It handles:

- Connection to Kafka brokers
- Asynchronous message sending
- Success and error handling
- Message compression

## Usage

### Consumer Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "syscall"
    
    "github.com/IBM/sarama"
    "github.com/rs/zerolog"
    "sceyt-search-service/pkg/kafka"
)

func main() {
    // Create a context that can be cancelled
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    // Setup zerolog
    logger := zerolog.New(os.Stdout).With().Timestamp().Str("component", "kafka-consumer").Logger()
    
    // Define your consumer configuration
    config := kafka.ConsumerConfig{
        Brokers: []string{"localhost:9092"},
        GroupID: "my-consumer-group",
        Topics:  []string{"my-topic"},
        Logger:  logger,
    }
    
    // Create a new consumer
    consumer, err := kafka.NewConsumer(config)
    if err != nil {
        logger.Fatal().Err(err).Msg("Error creating consumer")
    }
    
    // Define your message handler function
    consumer.SetMessageHandler(func(msg *sarama.ConsumerMessage) error {
        logger.Info().
            Str("topic", msg.Topic).
            Int32("partition", msg.Partition).
            Int64("offset", msg.Offset).
            Str("key", string(msg.Key)).
            Str("value", string(msg.Value)).
            Msg("Message received")
        return nil
    })
    
    // Start the consumer
    if err := consumer.Start(ctx); err != nil {
        logger.Fatal().Err(err).Msg("Error starting consumer")
    }
    
    // Wait for termination signal
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan
    
    // Close the consumer
    if err := consumer.Close(); err != nil {
        logger.Error().Err(err).Msg("Error closing consumer")
    }
}
```

### Producer Example

```go
package main

import (
    "os"
    
    "github.com/rs/zerolog"
    "sceyt-search-service/pkg/kafka"
)

func main() {
    // Setup zerolog
    logger := zerolog.New(os.Stdout).With().Timestamp().Str("component", "kafka-producer").Logger()
    
    // Define your producer configuration
    config := kafka.ProducerConfig{
        Brokers: []string{"localhost:9092"},
        Logger:  logger,
    }
    
    // Create a new producer
    producer, err := kafka.NewProducer(config)
    if err != nil {
        logger.Fatal().Err(err).Msg("Error creating producer")
    }
    defer producer.Close()
    
    // Send a message
    err = producer.SendMessage("my-topic", []byte("my-key"), []byte("Hello, Kafka!"), nil)
    if err != nil {
        logger.Fatal().Err(err).Msg("Error sending message")
    }
    
    logger.Info().Msg("Message sent successfully")
}
```

## Producer Methods

| Method | Description |
|--------|-------------|
| SendMessage(topic, key, value) | Sends a message with byte array key and value |

## Configuration Options

### Consumer Configuration

| Option | Description | Default |
|--------|-------------|---------|
| Brokers | List of Kafka broker addresses | Required |
| GroupID | Consumer group ID | Required |
| Topics | List of topics to consume | Required |
| Logger | zerolog.Logger instance | Default zerolog logger to stdout |
| SaramaConfig | Custom Sarama configuration | Default configuration with sensible values |

### Producer Configuration

| Option | Description | Default |
|--------|-------------|---------|
| Brokers | List of Kafka broker addresses | Required |
| Logger | zerolog.Logger instance | Default zerolog logger to stdout |
| SaramaConfig | Custom Sarama configuration | Default configuration with sensible values |

## Testing

The package includes comprehensive tests:

- Unit tests for both consumer and producer
- Integration tests that verify the interaction between producer and consumer
- Mock implementations for testing without a live Kafka broker

To run the tests:

```bash
# From the project root
# Run all tests
go test -v ./kafka

# Run integration tests (requires Kafka broker)
KAFKA_INTEGRATION_TEST=true go test -v ./kafka -run TestIntegration

# Or from within the kafka directory
# Run all tests
go test -v

# Run integration tests (requires Kafka broker)
KAFKA_INTEGRATION_TEST=true go test -v -run TestIntegration
```

## Error Handling

Both the consumer and producer components include robust error handling:

- The consumer monitors the consumer group errors channel
- The producer monitors the producer errors channel
- Both components log errors and can recover from transient failures
- The consumer will attempt to rejoin the consumer group if disconnected

## Dependencies

- [Sarama](https://github.com/IBM/sarama) - A Go client library for Apache Kafka 