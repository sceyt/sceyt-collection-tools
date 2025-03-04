package kafka

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
)

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	topics   []string
	brokers  []string
	groupID  string
	handler  func(message *sarama.ConsumerMessage) error
	logger   zerolog.Logger
	consumer sarama.ConsumerGroup
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// ConsumerConfig holds the configuration for a Kafka consumer
type ConsumerConfig struct {
	Brokers      []string
	GroupID      string
	Topics       []string
	Logger       zerolog.Logger
	SaramaConfig *sarama.Config
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	// Use default logger if none provided
	logger := config.Logger
	// Check if logger is the zero value by checking its output writer
	// A zerolog.Logger with no writer will have a disabled level
	if logger.GetLevel() == zerolog.Disabled {
		logger = zerolog.New(os.Stdout).With().Timestamp().Str("component", "kafka-consumer").Logger()
	}

	if config.SaramaConfig == nil {
		config.SaramaConfig = sarama.NewConfig()
		config.SaramaConfig.Consumer.Return.Errors = true
		config.SaramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
		config.SaramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
		config.SaramaConfig.Version = sarama.V3_3_1_0

		// Enable auto commit with a reasonable interval
		config.SaramaConfig.Consumer.Offsets.AutoCommit.Enable = true
		config.SaramaConfig.Consumer.Offsets.AutoCommit.Interval = 5 * time.Second

		// Set reasonable timeouts
		config.SaramaConfig.Net.DialTimeout = 30 * time.Second
		config.SaramaConfig.Net.ReadTimeout = 30 * time.Second
		config.SaramaConfig.Net.WriteTimeout = 30 * time.Second
		config.SaramaConfig.Consumer.Group.Session.Timeout = 30 * time.Second
		config.SaramaConfig.Consumer.Group.Heartbeat.Interval = 10 * time.Second
	}

	logger.Info().
		Strs("brokers", config.Brokers).
		Str("group", config.GroupID).
		Strs("topics", config.Topics).
		Msg("Connecting to Kafka brokers")

	// Validate configuration
	if err := config.SaramaConfig.Validate(); err != nil {
		logger.Error().Err(err).Msg("Invalid Sarama configuration")
		return nil, err
	}

	consumer, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, config.SaramaConfig)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create consumer group")
		return nil, err
	}

	logger.Info().Msg("Successfully created Kafka consumer group")

	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		topics:   config.Topics,
		brokers:  config.Brokers,
		groupID:  config.GroupID,
		logger:   logger,
		consumer: consumer,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// SetMessageHandler sets the handler function for processing messages
func (c *Consumer) SetMessageHandler(handler func(message *sarama.ConsumerMessage) error) {
	c.handler = handler
}

// Start begins consuming messages from Kafka
func (c *Consumer) Start(ctx context.Context) error {
	if c.handler == nil {
		return errors.New("no message handler defined")
	}

	c.logger.Info().Strs("topics", c.topics).Msg("Starting Kafka consumer")

	// Handle consumer errors
	go func() {
		for err := range c.consumer.Errors() {
			c.logger.Error().Err(err).Msg("Consumer error")
		}
	}()

	// Start the consumer loop
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for {
			// Check if context is cancelled
			select {
			case <-ctx.Done():
				c.logger.Info().Msg("Context cancelled, stopping consumer")
				return
			default:
			}

			// Create a handler that implements the ConsumerGroupHandler interface
			handler := consumerGroupHandler{
				logger:  c.logger,
				handler: c.handler,
			}

			// Consume joins a consumer group and starts consuming from the topics
			c.logger.Info().Msg("Joining consumer group...")
			err := c.consumer.Consume(ctx, c.topics, handler)
			if err != nil {
				c.logger.Error().Err(err).Msg("Error from consumer")

				// If context was cancelled, exit the loop
				if ctx.Err() != nil {
					return
				}

				// Wait before retrying
				time.Sleep(2 * time.Second)
				continue
			}

			// Check if context was cancelled
			if ctx.Err() != nil {
				return
			}

			c.logger.Info().Msg("Consumer group session ended, rebalancing")
		}
	}()

	return nil
}

// Close stops the consumer and closes connections
func (c *Consumer) Close() error {
	c.cancel()
	c.wg.Wait()
	return c.consumer.Close()
}

// consumerGroupHandler implements the ConsumerGroupHandler interface
type consumerGroupHandler struct {
	logger  zerolog.Logger
	handler func(message *sarama.ConsumerMessage) error
}

func (h consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.logger.Info().Msg("Consumer group session setup")
	return nil
}

func (h consumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.logger.Info().Msg("Consumer group session cleanup")
	return nil
}

func (h consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.logger.Info().
		Str("topic", claim.Topic()).
		Int32("partition", claim.Partition()).
		Msg("Starting to consume from topic")

	// Process messages
	for message := range claim.Messages() {
		h.logger.Debug().
			Str("topic", message.Topic).
			Int32("partition", message.Partition).
			Int64("offset", message.Offset).
			Str("key", string(message.Key)).
			Msg("Received message")

		// Process the message
		if err := h.handler(message); err != nil {
			h.logger.Error().Err(err).Msg("Error processing message")
		} else {
			// Mark the message as processed
			session.MarkMessage(message, "")
			h.logger.Debug().Int64("offset", message.Offset).Msg("Successfully processed message")
		}
	}

	return nil
}
