package config

import (
	"os"
	"strconv"
)

type Config struct {
	GatewayAddr               string
	MySQLDSN                  string
	RedisAddr                 string
	RedisPassword             string
	RedisDB                   int
	RabbitMQURL               string
	RabbitMQQueue             string
	StorageBackend            string
	CaseCacheTTLSeconds       int
	OutboxPollIntervalSeconds int
	OutboxBatchSize           int
	OutboxMaxAttempts         int
	OutboxRetryBaseSeconds    int
}

func LoadFromEnv() Config {
	return Config{
		GatewayAddr:               getEnv("GATEWAY_ADDR", ":8080"),
		MySQLDSN:                  getEnv("MYSQL_DSN", "trustops:trustops@tcp(127.0.0.1:3306)/content_quality?parseTime=true"),
		RedisAddr:                 getEnv("REDIS_ADDR", "127.0.0.1:6379"),
		RedisPassword:             getEnv("REDIS_PASSWORD", ""),
		RedisDB:                   getEnvInt("REDIS_DB", 0),
		RabbitMQURL:               getEnv("RABBITMQ_URL", "amqp://trustops:trustops@127.0.0.1:5672/"),
		RabbitMQQueue:             getEnv("RABBITMQ_QUEUE", "content.events.ingest"),
		StorageBackend:            getEnv("STORAGE_BACKEND", "auto"),
		CaseCacheTTLSeconds:       getEnvInt("CASE_CACHE_TTL_SECONDS", 300),
		OutboxPollIntervalSeconds: getEnvInt("OUTBOX_POLL_INTERVAL_SECONDS", 2),
		OutboxBatchSize:           getEnvInt("OUTBOX_BATCH_SIZE", 50),
		OutboxMaxAttempts:         getEnvInt("OUTBOX_MAX_ATTEMPTS", 3),
		OutboxRetryBaseSeconds:    getEnvInt("OUTBOX_RETRY_BASE_SECONDS", 2),
	}
}

func getEnv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func getEnvInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}
