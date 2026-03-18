package mq

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type CaseEvent struct {
	EventID     string   `json:"event_id"`
	CaseID      string   `json:"case_id"`
	ContentID   string   `json:"content_id"`
	ContentType string   `json:"content_type"`
	RiskSignals []string `json:"risk_signals"`
}

type Publisher interface {
	PublishCaseIngested(ctx context.Context, event CaseEvent) error
	Close() error
}

type NoopPublisher struct{}

func NewNoopPublisher() *NoopPublisher {
	return &NoopPublisher{}
}

func (p *NoopPublisher) PublishCaseIngested(_ context.Context, _ CaseEvent) error {
	return nil
}

func (p *NoopPublisher) Close() error {
	return nil
}

type RabbitPublisher struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string
}

func NewRabbitPublisher(url, queueName string) (*RabbitPublisher, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	return &RabbitPublisher{
		conn:      conn,
		channel:   ch,
		queueName: queueName,
	}, nil
}

func (p *RabbitPublisher) PublishCaseIngested(ctx context.Context, event CaseEvent) error {
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.channel.PublishWithContext(
		ctx,
		"",
		p.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        raw,
		},
	)
}

func (p *RabbitPublisher) Close() error {
	if p == nil {
		return nil
	}
	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
	return nil
}
