package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"time"

	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/config"
	httpapi "github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/http"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/mq"
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
	publisher, err := buildPublisher(cfg)
	if err != nil {
		log.Fatalf("rabbitmq publisher unavailable: %v", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			log.Printf("close publisher failed: %v", err)
		}
	}()

	h := server.Default(server.WithHostPorts(cfg.GatewayAddr))
	httpapi.RegisterRoutes(h, httpapi.Dependencies{
		CaseRepository: repo,
		Publisher:      publisher,
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
