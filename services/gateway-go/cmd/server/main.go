package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"math"
	"strings"
	"time"

	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/config"
	httpapi "github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/http"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/mq"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/security"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/storage"
	"github.com/cloudwego/hertz/pkg/app/server"
	_ "github.com/go-sql-driver/mysql"
	redis "github.com/redis/go-redis/v9"
)

type pingCloserDB = *sql.DB

type publisherWithClose interface {
	mq.Publisher
}

var openMySQL = func(dsn string) (pingCloserDB, error) {
	return sql.Open("mysql", dsn)
}

var pingRedis = func(ctx context.Context, cfg config.Config) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer rdb.Close()
	return rdb.Ping(ctx).Err()
}

var newRedisCacheClient = func(cfg config.Config) redis.Cmdable {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
}

var newRabbitPublisher = func(url, queueName string) (publisherWithClose, error) {
	return mq.NewRabbitPublisher(url, queueName)
}

func main() {
	cfg := config.LoadFromEnv()
	repo := buildRepository(cfg)
	limiter := buildRateLimiter(cfg)
	authorizer := security.NewAPIKeyAuthorizer(cfg.APIKeys())
	publisher, err := buildPublisher(cfg)
	if err != nil {
		log.Fatalf("rabbitmq publisher unavailable: %v", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			log.Printf("close publisher failed: %v", err)
		}
	}()
	startOutboxRelay(context.Background(), repo, publisher, cfg)

	h := server.Default(server.WithHostPorts(cfg.GatewayAddr))
	httpapi.RegisterRoutes(h, httpapi.Dependencies{
		CaseRepository: repo,
		Publisher:      publisher,
		Authorizer:     authorizer,
		RateLimiter:    limiter,
	})
	h.Spin()
}

func buildRepository(cfg config.Config) storage.CaseRepository {
	if cfg.StorageBackend == "memory" {
		log.Printf("gateway storage backend=memory")
		return storage.NewInMemoryCaseRepository(storage.DefaultSeedCases())
	}

	db, err := openMySQL(cfg.MySQLDSN)
	if err != nil {
		log.Printf("mysql open failed, fallback memory: %v", err)
		return storage.NewInMemoryCaseRepository(storage.DefaultSeedCases())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Printf("mysql ping failed, fallback memory: %v", err)
		_ = db.Close()
		return storage.NewInMemoryCaseRepository(storage.DefaultSeedCases())
	}

	if err := pingRedis(ctx, cfg); err != nil {
		log.Printf("redis ping failed, continue with mysql only: %v", err)
		log.Printf("gateway storage backend=mysql")
		return storage.NewMySQLRedisCaseRepository(db, nil, time.Duration(cfg.CaseCacheTTLSeconds)*time.Second)
	}

	rdb := newRedisCacheClient(cfg)
	log.Printf("gateway storage backend=mysql+redis")
	return storage.NewMySQLRedisCaseRepository(db, rdb, time.Duration(cfg.CaseCacheTTLSeconds)*time.Second)
}

func buildPublisher(cfg config.Config) (publisherWithClose, error) {
	if cfg.RabbitMQURL == "" {
		return nil, errors.New("rabbitmq url is empty")
	}
	pub, err := newRabbitPublisher(cfg.RabbitMQURL, cfg.RabbitMQQueue)
	if err != nil {
		return nil, err
	}
	log.Printf("gateway publisher backend=rabbitmq queue=%s", cfg.RabbitMQQueue)
	return pub, nil
}

func buildRateLimiter(cfg config.Config) security.RateLimiter {
	limit := cfg.RateLimitPerMinute
	if limit <= 0 {
		return security.NewNoopLimiter()
	}
	prefix := strings.TrimSpace(cfg.RateLimitPrefix)
	if prefix == "" {
		prefix = "cq"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pingRedis(ctx, cfg); err == nil {
		rdb := newRedisCacheClient(cfg)
		return security.NewRedisFixedWindowLimiter(rdb, prefix, limit, time.Minute)
	}

	log.Printf("rate limiter fallback=in-memory limit_per_minute=%d", limit)
	return security.NewInMemoryFixedWindowLimiter(limit, time.Minute)
}

func startOutboxRelay(ctx context.Context, repo storage.CaseRepository, publisher mq.Publisher, cfg config.Config) {
	pollInterval := time.Duration(cfg.OutboxPollIntervalSeconds) * time.Second
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	ticker := time.NewTicker(pollInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := processOutboxOnce(ctx, repo, publisher, cfg); err != nil {
					log.Printf("outbox relay error: %v", err)
				}
			}
		}
	}()
}

func processOutboxOnce(ctx context.Context, repo storage.CaseRepository, publisher mq.Publisher, cfg config.Config) error {
	batchSize := cfg.OutboxBatchSize
	if batchSize <= 0 {
		batchSize = 50
	}
	maxAttempts := cfg.OutboxMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	events, err := repo.ClaimPendingOutboxEvents(ctx, batchSize, time.Now().UTC())
	if err != nil {
		return err
	}
	for _, event := range events {
		pubErr := publisher.PublishCaseIngested(ctx, mq.CaseEvent{
			EventID:     event.EventID,
			CaseID:      event.CaseID,
			ContentID:   event.ContentID,
			ContentType: event.ContentType,
			RiskSignals: event.RiskSignals,
		})
		if pubErr == nil {
			if err := repo.MarkOutboxPublished(ctx, event.ID); err != nil {
				return err
			}
			continue
		}

		nextAttemptCount := event.Attempts + 1
		if nextAttemptCount >= maxAttempts {
			if err := repo.MarkOutboxDead(ctx, event.ID, nextAttemptCount, pubErr.Error()); err != nil {
				return err
			}
			continue
		}
		nextAttemptAt := time.Now().UTC().Add(calculateRetryDelay(nextAttemptCount, cfg.OutboxRetryBaseSeconds))
		if err := repo.MarkOutboxRetry(ctx, event.ID, nextAttemptCount, nextAttemptAt, pubErr.Error()); err != nil {
			return err
		}
	}
	return nil
}

func calculateRetryDelay(attempt, baseSeconds int) time.Duration {
	if baseSeconds <= 0 {
		baseSeconds = 2
	}
	if attempt <= 0 {
		attempt = 1
	}
	exponent := float64(attempt - 1)
	seconds := float64(baseSeconds) * math.Pow(2, exponent)
	if seconds > 3600 {
		seconds = 3600
	}
	return time.Duration(seconds) * time.Second
}
