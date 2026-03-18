package main

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"time"

	"github.com/Haimbeau1o/trustops-content-quality-platform/services/worker/internal/config"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/worker/internal/consumer"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/worker/internal/storage"
	_ "github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
)

type pingCloserDB = *sql.DB

var openMySQL = func(dsn string) (pingCloserDB, error) {
	return sql.Open("mysql", dsn)
}

func main() {
	cfg := config.LoadFromEnv()
	processedRepo, err := buildProcessedEventRepository(cfg)
	if err != nil {
		log.Fatalf("worker processed-event repository init failed: %v", err)
	}
	logger := log.Default()

	conn, err := amqp.Dial(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("connect rabbitmq failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("open channel failed: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		cfg.QueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("declare queue failed: %v", err)
	}

	msgs, err := ch.Consume(
		cfg.QueueName,
		cfg.ConsumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("consume queue failed: %v", err)
	}

	logger.Printf("worker started queue=%s consumer_tag=%s", cfg.QueueName, cfg.ConsumerTag)
	ctx := context.Background()
	for msg := range msgs {
		handleMessage(ctx, msg, processedRepo, logger, cfg.ConsumerTag)
	}
}

func buildProcessedEventRepository(cfg config.Config) (storage.ProcessedEventRepository, error) {
	if strings.ToLower(strings.TrimSpace(cfg.StorageBackend)) != "mysql" {
		return storage.NewInMemoryProcessedEventRepository(), nil
	}

	db, err := openMySQL(cfg.MySQLDSN)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return storage.NewMySQLProcessedEventRepository(db), nil
}

func handleMessage(ctx context.Context, msg amqp.Delivery, repo storage.ProcessedEventRepository, logger *log.Logger, consumerName string) {
	event, err := consumer.ParseCaseEvent(msg.Body)
	if err != nil {
		logger.Printf("invalid case event payload: %v", err)
		_ = msg.Nack(false, false)
		return
	}

	shouldProcess, err := repo.TryStart(ctx, event.EventID, event.CaseID, consumerName)
	if err != nil {
		logger.Printf("worker dedup acquire failed event_id=%s err=%v", event.EventID, err)
		_ = msg.Nack(false, false)
		return
	}
	if !shouldProcess {
		logger.Printf("worker skip duplicate event_id=%s case_id=%s", event.EventID, event.CaseID)
		_ = msg.Ack(false)
		return
	}

	if err := consumer.ProcessEvent(logger, event); err != nil {
		_ = repo.MarkFailed(ctx, event.EventID, err.Error())
		_ = msg.Nack(false, false)
		return
	}
	if err := repo.MarkProcessed(ctx, event.EventID); err != nil {
		logger.Printf("worker mark processed failed event_id=%s err=%v", event.EventID, err)
		_ = msg.Nack(false, false)
		return
	}

	_ = msg.Ack(false)
}
