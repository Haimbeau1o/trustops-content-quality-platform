package main

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/config"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/mq"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/security"
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

	repo, err := buildRepository(config.Config{
		StorageBackend:      "mysql",
		MySQLDSN:            "trustops:trustops@tcp(mysql:3306)/content_quality?parseTime=true",
		RedisAddr:           "redis:6379",
		CaseCacheTTLSeconds: 300,
	})
	if err != nil {
		t.Fatalf("buildRepository() error = %v", err)
	}

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

func TestBuildRepositoryFailsWhenMySQLUnavailable(t *testing.T) {
	prevOpenMySQL := openMySQL
	t.Cleanup(func() {
		openMySQL = prevOpenMySQL
	})

	openMySQL = func(string) (pingCloserDB, error) {
		return nil, errors.New("mysql unavailable")
	}

	repo, err := buildRepository(config.Config{
		StorageBackend: "mysql",
		MySQLDSN:       "invalid",
	})
	if err == nil {
		t.Fatalf("expected mysql-backed repository init to fail")
	}
	if repo != nil {
		t.Fatalf("expected nil repo when mysql init fails, got %T", repo)
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

type fakeOutboxRepo struct {
	events        []storage.OutboxEvent
	markedPublish []int64
	markedRetry   []int64
	markedDead    []int64
}

func (f *fakeOutboxRepo) SaveCase(context.Context, storage.Case) error {
	return nil
}

func (f *fakeOutboxRepo) GetCase(context.Context, string) (storage.Case, bool, error) {
	return storage.Case{}, false, nil
}

func (f *fakeOutboxRepo) IngestCase(context.Context, storage.IngestCaseInput) (storage.IngestCaseResult, error) {
	return storage.IngestCaseResult{}, nil
}

func (f *fakeOutboxRepo) ListAuditLogs(context.Context, string, int) ([]storage.AuditLog, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ClaimPendingOutboxEvents(context.Context, int, time.Time) ([]storage.OutboxEvent, error) {
	return append([]storage.OutboxEvent(nil), f.events...), nil
}

func (f *fakeOutboxRepo) MarkOutboxPublished(_ context.Context, outboxID int64) error {
	f.markedPublish = append(f.markedPublish, outboxID)
	return nil
}

func (f *fakeOutboxRepo) MarkOutboxRetry(_ context.Context, outboxID int64, _ int, _ time.Time, _ string) error {
	f.markedRetry = append(f.markedRetry, outboxID)
	return nil
}

func (f *fakeOutboxRepo) MarkOutboxDead(_ context.Context, outboxID int64, _ int, _ string) error {
	f.markedDead = append(f.markedDead, outboxID)
	return nil
}

func (f *fakeOutboxRepo) GetOpsMetrics(context.Context) (storage.OpsMetrics, error) {
	return storage.OpsMetrics{}, nil
}

type fakeRelayPublisher struct {
	publishErr error
}

func (f *fakeRelayPublisher) PublishCaseIngested(_ context.Context, _ mq.CaseEvent) error {
	return f.publishErr
}

func (f *fakeRelayPublisher) Close() error {
	return nil
}

func TestProcessOutboxOnceMarksPublishSuccess(t *testing.T) {
	repo := &fakeOutboxRepo{
		events: []storage.OutboxEvent{
			{
				ID:          11,
				EventID:     "evt-11",
				CaseID:      "case-11",
				ContentID:   "content-11",
				ContentType: "video",
				RiskSignals: []string{"spam"},
				Attempts:    0,
			},
		},
	}
	err := processOutboxOnce(context.Background(), repo, &fakeRelayPublisher{}, config.Config{
		OutboxBatchSize:        10,
		OutboxMaxAttempts:      3,
		OutboxRetryBaseSeconds: 2,
	})
	if err != nil {
		t.Fatalf("processOutboxOnce() error = %v", err)
	}
	if len(repo.markedPublish) != 1 || repo.markedPublish[0] != 11 {
		t.Fatalf("expected outbox id 11 to be marked published, got %#v", repo.markedPublish)
	}
}

func TestProcessOutboxOnceMarksDeadWhenAttemptsExceeded(t *testing.T) {
	repo := &fakeOutboxRepo{
		events: []storage.OutboxEvent{
			{
				ID:          22,
				EventID:     "evt-22",
				CaseID:      "case-22",
				ContentID:   "content-22",
				ContentType: "video",
				RiskSignals: []string{"unsafe"},
				Attempts:    2,
			},
		},
	}
	err := processOutboxOnce(context.Background(), repo, &fakeRelayPublisher{publishErr: errors.New("publish failed")}, config.Config{
		OutboxBatchSize:        10,
		OutboxMaxAttempts:      3,
		OutboxRetryBaseSeconds: 2,
	})
	if err != nil {
		t.Fatalf("processOutboxOnce() error = %v", err)
	}
	if len(repo.markedDead) != 1 || repo.markedDead[0] != 22 {
		t.Fatalf("expected outbox id 22 to be marked dead, got %#v", repo.markedDead)
	}
}

func TestBuildRateLimiterFallsBackToInMemory(t *testing.T) {
	prevPingRedis := pingRedis
	t.Cleanup(func() {
		pingRedis = prevPingRedis
	})
	pingRedis = func(context.Context, config.Config) error {
		return errors.New("redis unavailable")
	}

	limiter := buildRateLimiter(config.Config{
		RedisAddr:           "redis:6379",
		RateLimitPerMinute:  10,
		RateLimitPrefix:     "cq",
		CaseCacheTTLSeconds: 300,
	})
	if _, ok := limiter.(*security.InMemoryFixedWindowLimiter); !ok {
		t.Fatalf("expected in-memory limiter fallback, got %T", limiter)
	}
}

func TestBuildRateLimiterUsesRedisWhenAvailable(t *testing.T) {
	prevPingRedis := pingRedis
	t.Cleanup(func() {
		pingRedis = prevPingRedis
	})
	pingRedis = func(context.Context, config.Config) error {
		return nil
	}

	limiter := buildRateLimiter(config.Config{
		RedisAddr:           "redis:6379",
		RateLimitPerMinute:  10,
		RateLimitPrefix:     "cq",
		CaseCacheTTLSeconds: 300,
	})
	if _, ok := limiter.(*security.RedisFixedWindowLimiter); !ok {
		t.Fatalf("expected redis limiter, got %T", limiter)
	}
}
