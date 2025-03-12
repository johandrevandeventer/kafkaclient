package config

type KafkaProducerConfig struct {
	Brokers    []string
	MaxRetries int
	PoolSize   int
}

type KafkaConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string // For consumer groups
}

func NewKafkaProducerConfig(brokers []string, maxRetries int, poolSize int) *KafkaProducerConfig {
	return &KafkaProducerConfig{
		Brokers:    brokers,
		MaxRetries: maxRetries,
		PoolSize:   poolSize,
	}
}

func NewKafkaConsumerConfig(brokers []string, topic string, groupID string) *KafkaConsumerConfig {
	return &KafkaConsumerConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	}
}
