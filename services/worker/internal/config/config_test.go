package config

import "testing"

func TestLoadFromEnvDefaults(t *testing.T) {
	t.Setenv("WORKER_RABBITMQ_URL", "")
	t.Setenv("WORKER_QUEUE", "")
	t.Setenv("WORKER_CONSUMER_TAG", "")
	t.Setenv("WORKER_STORAGE_BACKEND", "")
	t.Setenv("WORKER_MYSQL_DSN", "")

	cfg := LoadFromEnv()

	if cfg.RabbitMQURL != "amqp://trustops:trustops@127.0.0.1:5672/" {
		t.Fatalf("unexpected default rabbitmq url: %q", cfg.RabbitMQURL)
	}
	if cfg.QueueName != "content.events.ingest" {
		t.Fatalf("unexpected default queue: %q", cfg.QueueName)
	}
	if cfg.ConsumerTag != "content-quality-worker" {
		t.Fatalf("unexpected default consumer tag: %q", cfg.ConsumerTag)
	}
	if cfg.StorageBackend != "memory" {
		t.Fatalf("unexpected default storage backend: %q", cfg.StorageBackend)
	}
	if cfg.MySQLDSN != "trustops:trustops@tcp(127.0.0.1:3306)/content_quality?parseTime=true" {
		t.Fatalf("unexpected default mysql dsn: %q", cfg.MySQLDSN)
	}
}

func TestLoadFromEnvOverrides(t *testing.T) {
	t.Setenv("WORKER_RABBITMQ_URL", "amqp://trustops:trustops@rabbitmq:5672/")
	t.Setenv("WORKER_QUEUE", "queue.custom")
	t.Setenv("WORKER_CONSUMER_TAG", "consumer.custom")
	t.Setenv("WORKER_STORAGE_BACKEND", "mysql")
	t.Setenv("WORKER_MYSQL_DSN", "trustops:trustops@tcp(mysql:3306)/content_quality?parseTime=true")

	cfg := LoadFromEnv()
	if cfg.RabbitMQURL != "amqp://trustops:trustops@rabbitmq:5672/" {
		t.Fatalf("unexpected rabbitmq url: %q", cfg.RabbitMQURL)
	}
	if cfg.QueueName != "queue.custom" {
		t.Fatalf("unexpected queue name: %q", cfg.QueueName)
	}
	if cfg.ConsumerTag != "consumer.custom" {
		t.Fatalf("unexpected consumer tag: %q", cfg.ConsumerTag)
	}
	if cfg.StorageBackend != "mysql" {
		t.Fatalf("unexpected storage backend: %q", cfg.StorageBackend)
	}
	if cfg.MySQLDSN != "trustops:trustops@tcp(mysql:3306)/content_quality?parseTime=true" {
		t.Fatalf("unexpected mysql dsn: %q", cfg.MySQLDSN)
	}
}
