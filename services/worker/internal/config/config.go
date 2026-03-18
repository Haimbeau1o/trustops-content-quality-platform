package config

import "os"

type Config struct {
	RabbitMQURL    string
	QueueName      string
	ConsumerTag    string
	StorageBackend string
	MySQLDSN       string
}

func LoadFromEnv() Config {
	return Config{
		RabbitMQURL:    getEnv("WORKER_RABBITMQ_URL", "amqp://trustops:trustops@127.0.0.1:5672/"),
		QueueName:      getEnv("WORKER_QUEUE", "content.events.ingest"),
		ConsumerTag:    getEnv("WORKER_CONSUMER_TAG", "content-quality-worker"),
		StorageBackend: getEnv("WORKER_STORAGE_BACKEND", "memory"),
		MySQLDSN:       getEnv("WORKER_MYSQL_DSN", "trustops:trustops@tcp(127.0.0.1:3306)/content_quality?parseTime=true"),
	}
}

func getEnv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}
