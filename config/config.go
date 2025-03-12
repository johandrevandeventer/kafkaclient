package config

type KafkaProducerConfig struct {
	Broker     string
	MaxRetries int
	PoolSize   int
}

type KafkaConsumerConfig struct {
	Broker  string
	Topic   string
	GroupID string // For consumer groups
}

func NewKafkaProducerConfig(brokers string, maxRetries int, poolSize int) *KafkaProducerConfig {
	return &KafkaProducerConfig{
		Broker:     brokers,
		MaxRetries: maxRetries,
		PoolSize:   poolSize,
	}
}

func NewKafkaConsumerConfig(brokers string, topic string, groupID string) *KafkaConsumerConfig {
	return &KafkaConsumerConfig{
		Broker:  brokers,
		Topic:   topic,
		GroupID: groupID,
	}
}
