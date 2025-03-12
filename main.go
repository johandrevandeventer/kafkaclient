package kafkaclient

import (
	"context"

	"github.com/johandrevandeventer/kafkaclient/config"
	"github.com/johandrevandeventer/kafkaclient/consumer"
	"github.com/johandrevandeventer/kafkaclient/producer"
	prometheusserver "github.com/johandrevandeventer/kafkaclient/prometheus"
	"go.uber.org/zap"
)

func NewProducerPool(cfg *config.KafkaProducerConfig, logger *zap.Logger) (*producer.KafkaProducerPool, error) {
	return producer.NewKafkaProducerPool(cfg, logger)
}

func NewConsumer(ctx context.Context, cfg *config.KafkaConsumerConfig, logger *zap.Logger) (*consumer.KafkaConsumer, error) {
	return consumer.NewKafkaConsumer(ctx, cfg, logger)
}

func StartPrometheus(address string) {
	go prometheusserver.StartPrometheusServer(address)
}
