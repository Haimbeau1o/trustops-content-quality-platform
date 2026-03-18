package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Haimbeau1o/trustops-content-quality-platform/services/worker/internal/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

type caseEvent struct {
	EventID string `json:"event_id"`
	CaseID  string `json:"case_id"`
}

func main() {
	cfg := config.LoadFromEnv()

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

	log.Printf("worker started queue=%s consumer_tag=%s", cfg.QueueName, cfg.ConsumerTag)
	ctx := context.Background()
	for msg := range msgs {
		handleMessage(ctx, msg)
	}
}

func handleMessage(_ context.Context, msg amqp.Delivery) {
	var event caseEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		log.Printf("invalid case event payload: %v", err)
		_ = msg.Nack(false, false)
		return
	}

	log.Printf("processed case event event_id=%s case_id=%s", event.EventID, event.CaseID)
	_ = msg.Ack(false)
}
