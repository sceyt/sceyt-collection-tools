package kafka

import (
	"errors"
	"os"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
)

// Producer represents a Sarama async producer
type Producer struct {
	producer sarama.AsyncProducer
	logger   zerolog.Logger
}

// ProducerConfig holds the configuration for a Kafka producer
type ProducerConfig struct {
	Brokers      []string
	Logger       zerolog.Logger
	SaramaConfig *sarama.Config
}

// NewProducer creates a new Kafka async producer
func NewProducer(config ProducerConfig) (*Producer, error) {
	// Use default logger if none provided
	logger := config.Logger
	// Check if logger is the zero value by checking its output writer
	if logger.GetLevel() == zerolog.Disabled {
		logger = zerolog.New(os.Stdout).With().Timestamp().Str("component", "kafka-producer").Logger()
	}

	if config.SaramaConfig == nil {
		config.SaramaConfig = sarama.NewConfig()
		config.SaramaConfig.Producer.RequiredAcks = sarama.WaitForLocal
		config.SaramaConfig.Producer.Compression = sarama.CompressionSnappy
		config.SaramaConfig.Producer.Flush.Frequency = 500
		config.SaramaConfig.Producer.Return.Successes = true
		config.SaramaConfig.Producer.Return.Errors = true
		config.SaramaConfig.Version = sarama.V3_3_1_0
	}

	logger.Info().Strs("brokers", config.Brokers).Msg("Connecting to Kafka brokers")

	// Validate configuration
	if err := config.SaramaConfig.Validate(); err != nil {
		logger.Error().Err(err).Msg("Invalid Sarama configuration")
		return nil, err
	}

	producer, err := sarama.NewAsyncProducer(config.Brokers, config.SaramaConfig)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create producer")
		return nil, err
	}

	// Start monitoring the success and error channels
	p := &Producer{
		producer: producer,
		logger:   logger,
	}

	// Start goroutines to handle success and error events
	go p.handleSuccesses()
	go p.handleErrors()

	logger.Info().Msg("Successfully created Kafka producer")
	return p, nil
}

// SendMessage sends a message to the specified topic
func (p *Producer) SendMessage(topic string, key, value []byte, headers []sarama.RecordHeader) error {
	if p.producer == nil {
		return errors.New("producer is not initialized")
	}

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.ByteEncoder(value),
		Headers: headers,
	}

	if key != nil {
		msg.Key = sarama.ByteEncoder(key)
	}

	p.producer.Input() <- msg
	return nil
}

// handleSuccesses processes successful message deliveries
func (p *Producer) handleSuccesses() {
	for success := range p.producer.Successes() {
		p.logger.Debug().
			Str("topic", success.Topic).
			Int32("partition", success.Partition).
			Int64("offset", success.Offset).
			Msg("Message sent successfully")
	}
}

// handleErrors processes message delivery errors
func (p *Producer) handleErrors() {
	for err := range p.producer.Errors() {
		p.logger.Error().
			Err(err.Err).
			Str("topic", err.Msg.Topic).
			Msg("Failed to send message")
	}
}

// Close closes the producer and releases resources
func (p *Producer) Close() error {
	if p.producer == nil {
		return nil
	}
	return p.producer.Close()
}
