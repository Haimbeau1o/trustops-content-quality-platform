package main

import (
	"context"
	"log"
	"testing"

	"github.com/Haimbeau1o/trustops-content-quality-platform/services/worker/internal/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeAcknowledger struct {
	acked  bool
	nacked bool
}

func (f *fakeAcknowledger) Ack(uint64, bool) error {
	f.acked = true
	return nil
}

func (f *fakeAcknowledger) Nack(uint64, bool, bool) error {
	f.nacked = true
	return nil
}

func (f *fakeAcknowledger) Reject(uint64, bool) error {
	f.nacked = true
	return nil
}

type fakeProcessedRepo struct {
	tryStart bool
	markedOK bool
}

func (f *fakeProcessedRepo) TryStart(context.Context, string, string, string) (bool, error) {
	return f.tryStart, nil
}

func (f *fakeProcessedRepo) MarkProcessed(context.Context, string) error {
	f.markedOK = true
	return nil
}

func (f *fakeProcessedRepo) MarkFailed(context.Context, string, string) error {
	return nil
}

func TestHandleMessageDuplicateIsAckedAndSkipped(t *testing.T) {
	repo := &fakeProcessedRepo{tryStart: false}
	ack := &fakeAcknowledger{}
	msg := amqp.Delivery{
		Acknowledger: ack,
		Body:         []byte(`{"event_id":"evt-1","case_id":"case-1"}`),
	}

	handleMessage(context.Background(), msg, repo, log.Default(), "worker-1")
	if !ack.acked {
		t.Fatalf("expected duplicate message to be acked")
	}
	if repo.markedOK {
		t.Fatalf("expected duplicate message not to mark processed")
	}
}

func TestHandleMessageSuccessIsMarkedAndAcked(t *testing.T) {
	repo := &fakeProcessedRepo{tryStart: true}
	ack := &fakeAcknowledger{}
	msg := amqp.Delivery{
		Acknowledger: ack,
		Body:         []byte(`{"event_id":"evt-2","case_id":"case-2"}`),
	}

	handleMessage(context.Background(), msg, repo, log.Default(), "worker-1")
	if !ack.acked {
		t.Fatalf("expected successful message to be acked")
	}
	if !repo.markedOK {
		t.Fatalf("expected successful message to mark processed")
	}
}

func TestBuildProcessedEventRepositoryFailsWhenMySQLUnavailable(t *testing.T) {
	prevOpenMySQL := openMySQL
	t.Cleanup(func() {
		openMySQL = prevOpenMySQL
	})
	openMySQL = func(string) (pingCloserDB, error) {
		return nil, context.DeadlineExceeded
	}

	repo, err := buildProcessedEventRepository(config.Config{
		StorageBackend: "mysql",
		MySQLDSN:       "invalid",
	})
	if err == nil {
		t.Fatalf("expected mysql-backed worker repository init to fail")
	}
	if repo != nil {
		t.Fatalf("expected nil repo on init failure, got %T", repo)
	}
}
