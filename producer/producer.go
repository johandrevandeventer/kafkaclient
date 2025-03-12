package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/johandrevandeventer/kafkaclient/config"
	"github.com/johandrevandeventer/kafkaclient/payload"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// Configurable parameters
// const (
// 	defaultTimeout = 10 * time.Second // Default timeout for sending messages
// 	maxRetries     = 5                // Maximum number of retries for sending messages
// )

// Metrics
var (
	messagesProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_producer_messages_produced_total",
		Help: "Total number of messages produced to Kafka",
	}, []string{"topic"})

	deliveryErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_producer_delivery_errors_total",
		Help: "Total number of message delivery errors",
	}, []string{"topic"})

	poolUsage = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_producer_pool_usage",
		Help: "Current usage of the Kafka producer pool",
	})
)

type KafkaProducerPool struct {
	producers    chan *kafka.Producer
	logger       *zap.Logger
	config       *config.KafkaProducerConfig
	deliveryChan chan kafka.Event
	wg           sync.WaitGroup
	maxRetries   int
}

func NewKafkaProducerPool(cfg *config.KafkaProducerConfig, logger *zap.Logger) (*KafkaProducerPool, error) {
	pool := &KafkaProducerPool{
		producers:    make(chan *kafka.Producer, cfg.PoolSize),
		config:       cfg,
		logger:       logger,
		deliveryChan: make(chan kafka.Event, 10000),
	}

	for i := 0; i < cfg.PoolSize; i++ {
		producer, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": cfg.Broker,
			"log_level":         0,
		})
		if err != nil {
			return nil, err
		}
		pool.producers <- producer
	}

	logger.Info("Kafka producer pool created successfully", zap.Int("pool_size", cfg.PoolSize))

	// Start handling delivery reports
	pool.wg.Add(1)
	go pool.handleDeliveryReports()

	return pool, nil
}

// handleDeliveryReports processes delivery reports from Kafka.
func (kpp *KafkaProducerPool) handleDeliveryReports() {
	defer kpp.wg.Done()
	for e := range kpp.deliveryChan {
		kpp.processDeliveryEvent(e)
	}
}

// processDeliveryEvent handles Kafka message delivery results.
func (kpp *KafkaProducerPool) processDeliveryEvent(e kafka.Event) {
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			kpp.logger.Error("Failed to deliver message",
				zap.String("kafka_topic", *ev.TopicPartition.Topic),
				zap.Error(ev.TopicPartition.Error),
			)
			deliveryErrors.WithLabelValues(*ev.TopicPartition.Topic).Inc()
			return
		}

		p, err := payload.Deserialize(ev.Value)
		if err != nil {
			kpp.logger.Error("Failed to deserialize payload",
				zap.Error(err),
			)
			deliveryErrors.WithLabelValues(*ev.TopicPartition.Topic).Inc()
			return
		}

		kpp.logger.Info("Message delivered",
			zap.String("kafka_topic", *ev.TopicPartition.Topic),
			zap.String("mqtt_topic", p.MqttTopic),
			zap.Int64("offset", int64(ev.TopicPartition.Offset)),
		)
		messagesProduced.WithLabelValues(*ev.TopicPartition.Topic).Inc()
	}
}

// Get retrieves a Kafka producer from the pool.
func (p *KafkaProducerPool) Get() *kafka.Producer {
	poolUsage.Inc()
	return <-p.producers
}

// Put returns a Kafka producer to the pool.
func (p *KafkaProducerPool) Put(producer *kafka.Producer) {
	poolUsage.Dec()
	p.producers <- producer
}

// SendMessage sends a message to Kafka with retries.
func (kpp *KafkaProducerPool) SendMessage(ctx context.Context, topic string, message []byte) error {
	producer := kpp.Get()
	defer kpp.Put(producer)

	// Validate the message structure
	// Deserialize the incoming message into a Payload struct
	p, err := payload.Deserialize(message)
	if err != nil {
		return fmt.Errorf("failed to deserialize payload: %w", err)
	}

	// Serialize the Payload struct into a byte slice (e.g., JSON)
	serializedPayload, err := p.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize payload: %w", err)
	}

	operation := func() error {
		errChan := make(chan error, 1)
		go func() {
			errChan <- producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          serializedPayload,
			}, kpp.deliveryChan)
		}()

		select {
		case err := <-errChan:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	retryBackoff := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(kpp.maxRetries))
	err = backoff.RetryNotify(operation, backoff.WithContext(retryBackoff, ctx), func(err error, duration time.Duration) {
		kpp.logger.Warn("Failed to send message, retrying...",
			zap.Error(err),
			zap.Duration("retry_after", duration),
		)
	})

	if err != nil {
		deliveryErrors.WithLabelValues(topic).Inc()
		return err
	}

	return nil
}

// Close shuts down the Kafka producer pool gracefully.
func (kpp *KafkaProducerPool) Close() {
	kpp.logger.Info("Closing Kafka producer pool...")

	// Wait for the delivery report handling goroutine to finish
	kpp.wg.Add(1)

	// Close the producer channel (only once)
	close(kpp.producers)

	// Wait for all the producers to be returned to the pool
	for producer := range kpp.producers {
		// Flush pending messages
		remaining := producer.Flush(5000) // 5-second timeout
		if remaining > 0 {
			kpp.logger.Warn("Failed to flush all messages", zap.Int("remaining_messages", remaining))
		} else {
			kpp.logger.Info("All messages flushed successfully")
		}

		// Close the producer after flushing
		producer.Close()
	}

	// Close the delivery channel after processing all delivery events
	close(kpp.deliveryChan)

	// Wait for the delivery report handling goroutine to finish processing
	kpp.wg.Wait()

	kpp.logger.Info("Kafka producer pool closed successfully")
}
