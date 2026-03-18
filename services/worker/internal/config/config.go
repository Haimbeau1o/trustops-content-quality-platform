package config

import "os"

type Config struct {
	RabbitMQURL string
	QueueName   string
	ConsumerTag string
}

func LoadFromEnv() Config {
	return Config{
		RabbitMQURL: getEnv("WORKER_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/"),
		QueueName:   getEnv("WORKER_QUEUE", "content.events.ingest"),
		ConsumerTag: getEnv("WORKER_CONSUMER_TAG", "content-quality-worker"),
	}
}

func getEnv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}
