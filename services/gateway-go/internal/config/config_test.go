package config

import (
	"os"
	"testing"
)

func TestLoadFromEnvDefaults(t *testing.T) {
	t.Setenv("GATEWAY_ADDR", "")
	t.Setenv("MYSQL_DSN", "")
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("REDIS_PASSWORD", "")
	t.Setenv("REDIS_DB", "")
	t.Setenv("RABBITMQ_URL", "")
	t.Setenv("RABBITMQ_QUEUE", "")
	t.Setenv("STORAGE_BACKEND", "")
	t.Setenv("CASE_CACHE_TTL_SECONDS", "")

	cfg := LoadFromEnv()

	if cfg.GatewayAddr != ":8080" {
		t.Fatalf("expected default gateway addr, got %q", cfg.GatewayAddr)
	}
	if cfg.MySQLDSN == "" {
		t.Fatalf("expected default mysql dsn")
	}
	if cfg.RedisAddr != "127.0.0.1:6379" {
		t.Fatalf("expected default redis addr, got %q", cfg.RedisAddr)
	}
	if cfg.RabbitMQURL != "amqp://trustops:trustops@127.0.0.1:5672/" {
		t.Fatalf("expected default rabbit url, got %q", cfg.RabbitMQURL)
	}
	if cfg.RabbitMQQueue != "content.events.ingest" {
		t.Fatalf("expected default rabbit queue, got %q", cfg.RabbitMQQueue)
	}
	if cfg.StorageBackend != "auto" {
		t.Fatalf("expected default storage backend, got %q", cfg.StorageBackend)
	}
	if cfg.CaseCacheTTLSeconds != 300 {
		t.Fatalf("expected default cache ttl 300, got %d", cfg.CaseCacheTTLSeconds)
	}
}

func TestLoadFromEnvOverrides(t *testing.T) {
	t.Setenv("GATEWAY_ADDR", ":18080")
	t.Setenv("MYSQL_DSN", "user:pass@tcp(mysql:3306)/db?parseTime=true")
	t.Setenv("REDIS_ADDR", "redis:6380")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("REDIS_DB", "5")
	t.Setenv("RABBITMQ_URL", "amqp://trustops:trustops@rabbitmq:5672/")
	t.Setenv("RABBITMQ_QUEUE", "q.demo")
	t.Setenv("STORAGE_BACKEND", "memory")
	t.Setenv("CASE_CACHE_TTL_SECONDS", "42")

	cfg := LoadFromEnv()

	if cfg.GatewayAddr != ":18080" {
		t.Fatalf("unexpected gateway addr: %q", cfg.GatewayAddr)
	}
	if cfg.MySQLDSN != "user:pass@tcp(mysql:3306)/db?parseTime=true" {
		t.Fatalf("unexpected mysql dsn: %q", cfg.MySQLDSN)
	}
	if cfg.RedisAddr != "redis:6380" || cfg.RedisPassword != "secret" || cfg.RedisDB != 5 {
		t.Fatalf("unexpected redis config: %#v", cfg)
	}
	if cfg.RabbitMQURL != "amqp://trustops:trustops@rabbitmq:5672/" || cfg.RabbitMQQueue != "q.demo" {
		t.Fatalf("unexpected rabbit config: %#v", cfg)
	}
	if cfg.StorageBackend != "memory" {
		t.Fatalf("unexpected backend: %q", cfg.StorageBackend)
	}
	if cfg.CaseCacheTTLSeconds != 42 {
		t.Fatalf("unexpected cache ttl: %d", cfg.CaseCacheTTLSeconds)
	}
}

func TestLoadFromEnvInvalidIntUsesDefault(t *testing.T) {
	t.Setenv("REDIS_DB", "invalid")
	t.Setenv("CASE_CACHE_TTL_SECONDS", "bad")

	cfg := LoadFromEnv()

	if cfg.RedisDB != 0 {
		t.Fatalf("expected redis db default, got %d", cfg.RedisDB)
	}
	if cfg.CaseCacheTTLSeconds != 300 {
		t.Fatalf("expected ttl default, got %d", cfg.CaseCacheTTLSeconds)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	_ = os.Unsetenv("GATEWAY_ADDR")
	_ = os.Unsetenv("MYSQL_DSN")
	_ = os.Unsetenv("REDIS_ADDR")
	_ = os.Unsetenv("REDIS_PASSWORD")
	_ = os.Unsetenv("REDIS_DB")
	_ = os.Unsetenv("RABBITMQ_URL")
	_ = os.Unsetenv("RABBITMQ_QUEUE")
	_ = os.Unsetenv("STORAGE_BACKEND")
	_ = os.Unsetenv("CASE_CACHE_TTL_SECONDS")
	os.Exit(code)
}
