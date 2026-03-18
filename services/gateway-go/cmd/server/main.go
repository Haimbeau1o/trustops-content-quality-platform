package main

import (
	"context"
	"database/sql"
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

func main() {
	cfg := config.LoadFromEnv()
	repo := buildRepository(cfg)
	publisher := buildPublisher(cfg)
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

	db, err := sql.Open("mysql", cfg.MySQLDSN)
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

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("redis ping failed, fallback memory: %v", err)
		_ = db.Close()
		_ = rdb.Close()
		return storage.NewInMemoryCaseRepository(storage.DefaultSeedCases())
	}

	log.Printf("gateway storage backend=mysql+redis")
	return storage.NewMySQLRedisCaseRepository(db, rdb, time.Duration(cfg.CaseCacheTTLSeconds)*time.Second)
}

func buildPublisher(cfg config.Config) mq.Publisher {
	pub, err := mq.NewRabbitPublisher(cfg.RabbitMQURL, cfg.RabbitMQQueue)
	if err != nil {
		log.Printf("rabbitmq unavailable, using noop publisher: %v", err)
		return mq.NewNoopPublisher()
	}
	log.Printf("gateway publisher backend=rabbitmq queue=%s", cfg.RabbitMQQueue)
	return pub
}
