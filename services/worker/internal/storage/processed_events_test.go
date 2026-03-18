package storage

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestInMemoryProcessedEventRepositoryDeduplicates(t *testing.T) {
	repo := NewInMemoryProcessedEventRepository()
	ctx := context.Background()

	first, err := repo.TryStart(ctx, "evt-1", "case-1", "worker-a")
	if err != nil {
		t.Fatalf("TryStart() first error = %v", err)
	}
	if !first {
		t.Fatalf("expected first TryStart to return true")
	}

	second, err := repo.TryStart(ctx, "evt-1", "case-1", "worker-a")
	if err != nil {
		t.Fatalf("TryStart() second error = %v", err)
	}
	if second {
		t.Fatalf("expected duplicate TryStart to return false")
	}
}

func TestMySQLProcessedEventRepositoryLifecycle(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := NewMySQLProcessedEventRepository(db)
	ctx := context.Background()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO content_worker_processed_events (event_id, case_id, consumer_name, status, last_error, processed_at) VALUES (?, ?, ?, 'processing', '', NULL)")).
		WithArgs("evt-9", "case-9", "worker-1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	ok, err := repo.TryStart(ctx, "evt-9", "case-9", "worker-1")
	if err != nil {
		t.Fatalf("TryStart() error = %v", err)
	}
	if !ok {
		t.Fatalf("expected TryStart to return true for first insert")
	}

	mock.ExpectExec(regexp.QuoteMeta("UPDATE content_worker_processed_events SET status='processed', last_error='', processed_at=UTC_TIMESTAMP(), updated_at=UTC_TIMESTAMP() WHERE event_id=?")).
		WithArgs("evt-9").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := repo.MarkProcessed(ctx, "evt-9"); err != nil {
		t.Fatalf("MarkProcessed() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta("UPDATE content_worker_processed_events SET status='failed', last_error=?, updated_at=UTC_TIMESTAMP() WHERE event_id=?")).
		WithArgs("boom", "evt-9").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := repo.MarkFailed(ctx, "evt-9", "boom"); err != nil {
		t.Fatalf("MarkFailed() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations not met: %v", err)
	}
}
