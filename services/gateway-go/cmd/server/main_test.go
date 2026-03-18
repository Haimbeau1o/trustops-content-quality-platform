package main

import (
	"context"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/config"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/storage"
)

func TestBuildRepositoryKeepsMySQLPersistenceWhenRedisUnavailable(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	mock.ExpectPing()

	prevOpenMySQL := openMySQL
	prevPingRedis := pingRedis
	t.Cleanup(func() {
		openMySQL = prevOpenMySQL
		pingRedis = prevPingRedis
	})

	openMySQL = func(string) (pingCloserDB, error) {
		return db, nil
	}
	pingRedis = func(context.Context, config.Config) error {
		return errors.New("redis unavailable")
	}

	repo := buildRepository(config.Config{
		StorageBackend:      "mysql",
		MySQLDSN:            "trustops:trustops@tcp(mysql:3306)/content_quality?parseTime=true",
		RedisAddr:           "redis:6379",
		CaseCacheTTLSeconds: 300,
	})

	if _, ok := repo.(*storage.InMemoryCaseRepository); ok {
		t.Fatalf("expected mysql-backed repository when redis is unavailable")
	}
	if _, ok := repo.(*storage.MySQLRedisCaseRepository); !ok {
		t.Fatalf("expected mysql redis repository, got %T", repo)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations not met: %v", err)
	}
}

func TestBuildPublisherFailsWhenRabbitMQUnavailable(t *testing.T) {
	prevNewRabbitPublisher := newRabbitPublisher
	t.Cleanup(func() {
		newRabbitPublisher = prevNewRabbitPublisher
	})

	newRabbitPublisher = func(string, string) (publisherWithClose, error) {
		return nil, errors.New("rabbitmq unavailable")
	}

	_, err := buildPublisher(config.Config{
		RabbitMQURL:   "amqp://trustops:trustops@rabbitmq:5672/",
		RabbitMQQueue: "content.events.ingest",
	})
	if err == nil {
		t.Fatalf("expected publisher initialization to fail")
	}
}
