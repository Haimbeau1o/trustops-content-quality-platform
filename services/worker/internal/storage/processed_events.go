package storage

import (
	"context"
	"database/sql"
	"sync"
)

type ProcessedEventRepository interface {
	TryStart(ctx context.Context, eventID, caseID, consumerName string) (bool, error)
	MarkProcessed(ctx context.Context, eventID string) error
	MarkFailed(ctx context.Context, eventID, lastError string) error
}

type InMemoryProcessedEventRepository struct {
	mu     sync.Mutex
	events map[string]string
}

func NewInMemoryProcessedEventRepository() *InMemoryProcessedEventRepository {
	return &InMemoryProcessedEventRepository{
		events: make(map[string]string),
	}
}

func (r *InMemoryProcessedEventRepository) TryStart(_ context.Context, eventID, _ string, _ string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if status, exists := r.events[eventID]; exists && status != "failed" {
		return false, nil
	}
	r.events[eventID] = "processing"
	return true, nil
}

func (r *InMemoryProcessedEventRepository) MarkProcessed(_ context.Context, eventID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events[eventID] = "processed"
	return nil
}

func (r *InMemoryProcessedEventRepository) MarkFailed(_ context.Context, eventID, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events[eventID] = "failed"
	return nil
}

type MySQLProcessedEventRepository struct {
	db *sql.DB
}

func NewMySQLProcessedEventRepository(db *sql.DB) *MySQLProcessedEventRepository {
	return &MySQLProcessedEventRepository{db: db}
}

func (r *MySQLProcessedEventRepository) TryStart(ctx context.Context, eventID, caseID, consumerName string) (bool, error) {
	result, err := r.db.ExecContext(
		ctx,
		`INSERT INTO content_worker_processed_events (event_id, case_id, consumer_name, status, last_error, processed_at)
		VALUES (?, ?, ?, 'processing', '', NULL)
		ON DUPLICATE KEY UPDATE
			status = IF(status = 'failed', 'processing', status),
			consumer_name = IF(status = 'failed', VALUES(consumer_name), consumer_name),
			last_error = IF(status = 'failed', '', last_error),
			processed_at = IF(status = 'failed', NULL, processed_at)`,
		eventID,
		caseID,
		consumerName,
	)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (r *MySQLProcessedEventRepository) MarkProcessed(ctx context.Context, eventID string) error {
	_, err := r.db.ExecContext(
		ctx,
		"UPDATE content_worker_processed_events SET status='processed', last_error='', processed_at=UTC_TIMESTAMP(), updated_at=UTC_TIMESTAMP() WHERE event_id=?",
		eventID,
	)
	return err
}

func (r *MySQLProcessedEventRepository) MarkFailed(ctx context.Context, eventID, lastError string) error {
	_, err := r.db.ExecContext(
		ctx,
		"UPDATE content_worker_processed_events SET status='failed', last_error=?, updated_at=UTC_TIMESTAMP() WHERE event_id=?",
		lastError,
		eventID,
	)
	return err
}
