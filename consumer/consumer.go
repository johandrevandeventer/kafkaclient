package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/johandrevandeventer/kafkaclient/config"
	"github.com/johandrevandeventer/kafkaclient/payload"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// Configurable parameters
const (
	defaultPollTimeout = 10 * time.Millisecond // Default timeout for polling messages
	defaultChannelSize = 1000                  // Default size of the output channel
	monitorInterval    = 60 * time.Second      // Interval for monitoring channel usage
	pauseResumeDelay   = 1 * time.Second       // Delay before resuming consumer after pause
)

type partitionOffsets struct {
	low  int64
	high int64
}

// Metrics
var (
	messagesConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consumer_messages_consumed_total",
		Help: "Total number of messages consumed from Kafka",
	}, []string{"topic"})

	consumeErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consumer_errors_total",
		Help: "Total number of Kafka consumer errors",
	}, []string{"topic"})

	channelUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_channel_usage",
		Help: "Current usage of the output channel",
	}, []string{"channel"})

	consumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Current consumer lag in messages",
	}, []string{"topic", "partition"})
)

type KafkaConsumer struct {
	consumer      *kafka.Consumer
	topic         string
	ctx           context.Context
	logger        *zap.Logger
	outputChannel chan []byte
	wg            sync.WaitGroup
}

func NewKafkaConsumer(ctx context.Context, cfg *config.KafkaConsumerConfig, logger *zap.Logger) (*KafkaConsumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Broker,
		"log_level":         0,
		"group.id":          cfg.GroupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}

	logger.Info("Kafka consumer created successfully")

	kc := &KafkaConsumer{
		consumer:      consumer,
		topic:         cfg.Topic,
		ctx:           ctx,
		logger:        logger,
		outputChannel: make(chan []byte, defaultChannelSize),
	}

	// Start channel usage monitor
	kc.wg.Add(1)
	go kc.monitorChannelUsage()

	return kc, nil
}

// monitorChannelUsage logs the length of the output channels every 60 seconds.
func (kc *KafkaConsumer) monitorChannelUsage() {
	defer kc.wg.Done()
	ticker := time.NewTicker(monitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-kc.ctx.Done():
			kc.logger.Info("Stopping channel usage monitor")
			return
		case <-ticker.C:
			channelUsage.WithLabelValues("output").Set(float64(len(kc.outputChannel)))
			kc.logger.Info("Channel usage",
				zap.Int("output_data_channel_len", len(kc.outputChannel)),
			)
		}
	}
}

func (kc *KafkaConsumer) Start() error {
	// Subscribe to Kafka topic
	err := kc.consumer.SubscribeTopics([]string{kc.topic}, nil)
	if err != nil {
		return err
	}

	kc.logger.Info("Successfully subscribed to Kafka topics", zap.Strings("topics", []string{kc.topic}))

	// Start the lag tracker
	kc.wg.Add(1)
	go kc.startLagTracker()

	kc.wg.Add(1)
	go kc.consumeMessages()

	return nil
}

// consumeMessages starts consuming messages from the Kafka topic.
func (kc *KafkaConsumer) consumeMessages() {
	defer kc.wg.Done()
	for {
		select {
		case <-kc.ctx.Done():
			kc.logger.Info("Stopping message consumption")
			return
		default:
			// Pause consumer if channels are full
			if len(kc.outputChannel) == cap(kc.outputChannel) {
				kc.consumer.Pause([]kafka.TopicPartition{{Topic: &kc.topic, Partition: kafka.PartitionAny}})
				kc.logger.Warn("Channels full, pausing consumer")

				// Add a delay before checking again
				select {
				case <-time.After(pauseResumeDelay):
				case <-kc.ctx.Done():
					return
				}
				continue
			}

			// Resume consumer if channels have space and it was paused
			if len(kc.outputChannel) < cap(kc.outputChannel) {
				kc.consumer.Resume([]kafka.TopicPartition{{Topic: &kc.topic, Partition: kafka.PartitionAny}})
			}

			ev := kc.consumer.Poll(int(defaultPollTimeout.Milliseconds()))
			switch e := ev.(type) {
			case *kafka.Message:
				p, err := payload.Deserialize(e.Value)
				if err != nil {
					kc.logger.Error("Failed to deserialize payload", zap.Error(err))
					consumeErrors.WithLabelValues(*e.TopicPartition.Topic).Inc()
					continue
				}

				if p.MqttTopic == "" {
					kc.logger.Info("Received message",
						zap.String("kafka_topic", *e.TopicPartition.Topic),
						zap.Int64("offset", int64(e.TopicPartition.Offset)),
					)
				} else {
					kc.logger.Info("Received message",
						zap.String("kafka_topic", *e.TopicPartition.Topic),
						zap.String("mqtt_topic", p.MqttTopic),
						zap.Int64("offset", int64(e.TopicPartition.Offset)),
					)
				}
				messagesConsumed.WithLabelValues(*e.TopicPartition.Topic).Inc()

				// Serialize the Payload struct into a byte slice (e.g., JSON)
				serializedPayload, err := p.Serialize()
				if err != nil {
					return
				}

				// Ensure messages are written to the channels
				select {
				case kc.outputChannel <- serializedPayload:
				default:
					kc.logger.Warn("Output channel is full, dropping message")
					consumeErrors.WithLabelValues(*e.TopicPartition.Topic).Inc()
				}
			case kafka.PartitionEOF:
				kc.logger.Info("Reached end of partition", zap.String("topic", *e.Topic))
			case kafka.Error:
				kc.logger.Error("Kafka error", zap.Error(e))
				consumeErrors.WithLabelValues(kc.topic).Inc()
			}
		}
	}
}

func (kc *KafkaConsumer) startLagTracker() {
	defer kc.wg.Done()

	ticker := time.NewTicker(30 * time.Second) // Adjust the interval as needed
	defer ticker.Stop()

	// Cache for watermark offsets
	watermarkCache := make(map[kafka.TopicPartition]partitionOffsets)

	for {
		select {
		case <-kc.ctx.Done():
			kc.logger.Info("Stopping lag tracker")
			return
		case <-ticker.C:
			partitions, err := kc.consumer.Assignment()
			if err != nil {
				kc.logger.Error("Failed to get assigned partitions", zap.Error(err))
				continue
			}

			// Update watermark cache
			for _, partition := range partitions {
				low, high, err := kc.consumer.QueryWatermarkOffsets(*partition.Topic, partition.Partition, 1000)
				if err != nil {
					kc.logger.Error("Failed to query watermark offsets",
						zap.String("topic", *partition.Topic),
						zap.Int32("partition", partition.Partition),
						zap.Error(err),
					)
					continue
				}
				watermarkCache[partition] = partitionOffsets{low: low, high: high}
			}

			// Track lag using cached watermarks
			for partition, offsets := range watermarkCache {
				offset, err := kc.consumer.Position([]kafka.TopicPartition{partition})
				if err != nil {
					kc.logger.Error("Failed to get current offset",
						zap.String("topic", *partition.Topic),
						zap.Int32("partition", partition.Partition),
						zap.Error(err),
					)
					continue
				}

				lag := offsets.high - int64(offset[0].Offset)

				// Update Prometheus gauge
				consumerLag.WithLabelValues(*partition.Topic, string(partition.Partition)).Set(float64(lag))
			}
		}
	}
}

// Close shuts down the Kafka consumer gracefully.
func (kc *KafkaConsumer) Close() {
	kc.logger.Info("Closing Kafka consumer...")

	// Close the Kafka consumer
	kc.consumer.Close()

	// Close the output channel
	close(kc.outputChannel)

	// Wait for all goroutines to finish
	kc.wg.Wait()
}

// GetOutputChannel returns the output channel for the Kafka consumer.
func (kc *KafkaConsumer) GetOutputChannel() <-chan []byte {
	return kc.outputChannel
}
