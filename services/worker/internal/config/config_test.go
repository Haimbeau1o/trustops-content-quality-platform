package config

import "testing"

func TestLoadFromEnvDefaults(t *testing.T) {
	t.Setenv("WORKER_RABBITMQ_URL", "")
	t.Setenv("WORKER_QUEUE", "")
	t.Setenv("WORKER_CONSUMER_TAG", "")

	cfg := LoadFromEnv()

	if cfg.RabbitMQURL != "amqp://guest:guest@127.0.0.1:5672/" {
		t.Fatalf("unexpected default rabbitmq url: %q", cfg.RabbitMQURL)
	}
	if cfg.QueueName != "content.events.ingest" {
		t.Fatalf("unexpected default queue: %q", cfg.QueueName)
	}
	if cfg.ConsumerTag != "content-quality-worker" {
		t.Fatalf("unexpected default consumer tag: %q", cfg.ConsumerTag)
	}
}

func TestLoadFromEnvOverrides(t *testing.T) {
	t.Setenv("WORKER_RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
	t.Setenv("WORKER_QUEUE", "queue.custom")
	t.Setenv("WORKER_CONSUMER_TAG", "consumer.custom")

	cfg := LoadFromEnv()
	if cfg.RabbitMQURL != "amqp://guest:guest@rabbitmq:5672/" {
		t.Fatalf("unexpected rabbitmq url: %q", cfg.RabbitMQURL)
	}
	if cfg.QueueName != "queue.custom" {
		t.Fatalf("unexpected queue name: %q", cfg.QueueName)
	}
	if cfg.ConsumerTag != "consumer.custom" {
		t.Fatalf("unexpected consumer tag: %q", cfg.ConsumerTag)
	}
}
