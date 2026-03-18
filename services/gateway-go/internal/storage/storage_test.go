package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestInMemoryRepositorySaveAndGet(t *testing.T) {
	repo := NewInMemoryCaseRepository(nil)
	ctx := context.Background()

	want := Case{
		CaseID:      "case-evt-1001",
		Status:      "queued_for_review",
		ContentID:   "content-001",
		ContentType: "video",
		RiskSignals: []string{"spam"},
		Evidence: []EvidenceItem{
			{Type: "rule_hit", Detail: "rule:spam_keyword"},
		},
	}

	if err := repo.SaveCase(ctx, want); err != nil {
		t.Fatalf("SaveCase() error = %v", err)
	}

	got, found, err := repo.GetCase(ctx, want.CaseID)
	if err != nil {
		t.Fatalf("GetCase() error = %v", err)
	}
	if !found {
		t.Fatalf("expected case to be found")
	}
	if got.CaseID != want.CaseID || got.Status != want.Status || got.ContentID != want.ContentID {
		t.Fatalf("unexpected case got %#v", got)
	}
}

func TestMySQLRedisRepositorySavePersistsAndCaches(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer rdb.Close()

	repo := NewMySQLRedisCaseRepository(db, rdb, time.Minute)
	ctx := context.Background()

	c := Case{
		CaseID:      "case-evt-2001",
		Status:      "queued_for_review",
		ContentID:   "content-2001",
		ContentType: "image",
		RiskSignals: []string{"low_quality"},
		Evidence: []EvidenceItem{
			{Type: "rule_hit", Detail: "rule:blur"},
		},
	}

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO content_cases")).
		WithArgs(c.CaseID, c.Status, c.ContentID, c.ContentType, `["low_quality"]`, `[{"type":"rule_hit","detail":"rule:blur"}]`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.SaveCase(ctx, c); err != nil {
		t.Fatalf("SaveCase() error = %v", err)
	}

	cachePayload, err := mini.Get("cq:case:" + c.CaseID)
	if err != nil {
		t.Fatalf("expected cache entry: %v", err)
	}
	var cached Case
	if err := json.Unmarshal([]byte(cachePayload), &cached); err != nil {
		t.Fatalf("cache payload json error: %v", err)
	}
	if cached.CaseID != c.CaseID {
		t.Fatalf("unexpected cached case id: %q", cached.CaseID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations not met: %v", err)
	}
}

func TestMySQLRedisRepositoryGetLoadsFromCacheFirst(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer rdb.Close()

	repo := NewMySQLRedisCaseRepository(db, rdb, time.Minute)
	ctx := context.Background()

	cached := Case{
		CaseID:      "case-cache-001",
		Status:      "open",
		ContentID:   "content-cache",
		ContentType: "video",
		RiskSignals: []string{"spam"},
	}
	payload, _ := json.Marshal(cached)
	mini.Set("cq:case:"+cached.CaseID, string(payload))

	got, found, err := repo.GetCase(ctx, cached.CaseID)
	if err != nil {
		t.Fatalf("GetCase() error = %v", err)
	}
	if !found {
		t.Fatalf("expected cached case to be found")
	}
	if got.CaseID != cached.CaseID || got.Status != cached.Status {
		t.Fatalf("unexpected case loaded from cache: %#v", got)
	}
}

func TestMySQLRedisRepositoryGetFallsBackToDBAndBackfillsCache(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer rdb.Close()

	repo := NewMySQLRedisCaseRepository(db, rdb, time.Minute)
	ctx := context.Background()

	rows := sqlmock.NewRows([]string{"case_id", "status", "content_id", "content_type", "risk_signals", "evidence"}).
		AddRow("case-db-001", "open", "content-db", "video", `["abuse"]`, `[{"type":"rule_hit","detail":"rule:abuse"}]`)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT case_id, status, content_id, content_type, risk_signals, evidence FROM content_cases WHERE case_id = ?")).
		WithArgs("case-db-001").
		WillReturnRows(rows)

	got, found, err := repo.GetCase(ctx, "case-db-001")
	if err != nil {
		t.Fatalf("GetCase() error = %v", err)
	}
	if !found {
		t.Fatalf("expected db case to be found")
	}
	if got.CaseID != "case-db-001" || got.ContentID != "content-db" {
		t.Fatalf("unexpected db case: %#v", got)
	}

	if _, err := mini.Get("cq:case:case-db-001"); err != nil {
		t.Fatalf("expected cache backfill: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations not met: %v", err)
	}
}

func openSQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	return db, mock
}
