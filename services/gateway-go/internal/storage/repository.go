package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type EvidenceItem struct {
	Type   string `json:"type"`
	Detail string `json:"detail"`
}

type Case struct {
	CaseID      string         `json:"case_id"`
	Status      string         `json:"status"`
	ContentID   string         `json:"content_id"`
	ContentType string         `json:"content_type"`
	RiskSignals []string       `json:"risk_signals"`
	Evidence    []EvidenceItem `json:"evidence"`
}

type IngestCaseInput struct {
	IdempotencyKey string
	EventID        string
	ContentID      string
	ContentType    string
	RiskSignals    []string
	Evidence       []EvidenceItem
}

type IngestCaseResult struct {
	Case             Case
	IdempotentReplay bool
}

type OutboxEvent struct {
	ID          int64
	EventID     string
	CaseID      string
	ContentID   string
	ContentType string
	RiskSignals []string
	Attempts    int
	Status      string
	NextAttempt time.Time
	LastError   string
}

type OpsMetrics struct {
	TotalCases        int64 `json:"total_cases"`
	IdempotentReplays int64 `json:"idempotent_replays"`
	OutboxPending     int64 `json:"outbox_pending"`
	OutboxDeadLetter  int64 `json:"outbox_dead_letter"`
	AuditLogCount     int64 `json:"audit_log_count"`
}

type AuditLog struct {
	EventID   string    `json:"event_id"`
	CaseID    string    `json:"case_id"`
	Action    string    `json:"action"`
	Detail    string    `json:"detail"`
	CreatedAt time.Time `json:"created_at"`
}

type CaseRepository interface {
	SaveCase(ctx context.Context, c Case) error
	GetCase(ctx context.Context, caseID string) (Case, bool, error)
	IngestCase(ctx context.Context, input IngestCaseInput) (IngestCaseResult, error)
	ListAuditLogs(ctx context.Context, caseID string, limit int) ([]AuditLog, error)
	ClaimPendingOutboxEvents(ctx context.Context, limit int, now time.Time) ([]OutboxEvent, error)
	MarkOutboxPublished(ctx context.Context, outboxID int64) error
	MarkOutboxRetry(ctx context.Context, outboxID int64, attempts int, nextAttempt time.Time, lastError string) error
	MarkOutboxDead(ctx context.Context, outboxID int64, attempts int, lastError string) error
	GetOpsMetrics(ctx context.Context) (OpsMetrics, error)
}

type InMemoryCaseRepository struct {
	mu                sync.RWMutex
	cases             map[string]Case
	idempotencyByKey  map[string]string
	outboxByID        map[int64]OutboxEvent
	outboxSeq         int64
	idempotentReplays int64
	auditLogCount     int64
	auditLogsByCase   map[string][]AuditLog
}

func NewInMemoryCaseRepository(seed []Case) *InMemoryCaseRepository {
	m := map[string]Case{}
	for _, c := range seed {
		m[c.CaseID] = c
	}
	return &InMemoryCaseRepository{
		cases:            m,
		idempotencyByKey: map[string]string{},
		outboxByID:       map[int64]OutboxEvent{},
		auditLogsByCase:  map[string][]AuditLog{},
	}
}

func (r *InMemoryCaseRepository) SaveCase(_ context.Context, c Case) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cases[c.CaseID] = c
	return nil
}

func (r *InMemoryCaseRepository) GetCase(_ context.Context, caseID string) (Case, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.cases[caseID]
	return c, ok, nil
}

func (r *InMemoryCaseRepository) IngestCase(_ context.Context, input IngestCaseInput) (IngestCaseResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := normalizeIdempotencyKey(input)
	if key == "" {
		return IngestCaseResult{}, errors.New("idempotency_key_required")
	}

	if existingCaseID, ok := r.idempotencyByKey[key]; ok {
		existingCase := r.cases[existingCaseID]
		r.idempotentReplays++
		r.appendAuditLog(existingCaseID, AuditLog{
			EventID:   input.EventID,
			CaseID:    existingCaseID,
			Action:    "idempotent_replay",
			Detail:    "duplicate event replayed",
			CreatedAt: time.Now().UTC(),
		})
		return IngestCaseResult{
			Case:             existingCase,
			IdempotentReplay: true,
		}, nil
	}

	caseID := "case-" + input.EventID
	newCase := Case{
		CaseID:      caseID,
		Status:      "queued_for_review",
		ContentID:   input.ContentID,
		ContentType: input.ContentType,
		RiskSignals: append([]string(nil), input.RiskSignals...),
		Evidence:    append([]EvidenceItem(nil), input.Evidence...),
	}
	r.cases[caseID] = newCase
	r.idempotencyByKey[key] = caseID
	r.appendAuditLog(caseID, AuditLog{
		EventID:   input.EventID,
		CaseID:    caseID,
		Action:    "ingest_accepted",
		Detail:    "new ingest accepted and queued",
		CreatedAt: time.Now().UTC(),
	})

	r.outboxSeq++
	r.outboxByID[r.outboxSeq] = OutboxEvent{
		ID:          r.outboxSeq,
		EventID:     input.EventID,
		CaseID:      caseID,
		ContentID:   input.ContentID,
		ContentType: input.ContentType,
		RiskSignals: append([]string(nil), input.RiskSignals...),
		Attempts:    0,
		Status:      "pending",
		NextAttempt: time.Now().UTC(),
	}

	return IngestCaseResult{
		Case:             newCase,
		IdempotentReplay: false,
	}, nil
}

func (r *InMemoryCaseRepository) ListAuditLogs(_ context.Context, caseID string, limit int) ([]AuditLog, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entries := append([]AuditLog(nil), r.auditLogsByCase[caseID]...)
	if limit <= 0 || limit > len(entries) {
		limit = len(entries)
	}
	result := make([]AuditLog, 0, limit)
	for i := 0; i < limit; i++ {
		idx := len(entries) - 1 - i
		if idx < 0 {
			break
		}
		result = append(result, entries[idx])
	}
	return result, nil
}

func (r *InMemoryCaseRepository) ClaimPendingOutboxEvents(_ context.Context, limit int, now time.Time) ([]OutboxEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if limit <= 0 {
		limit = 1
	}

	ids := make([]int64, 0, len(r.outboxByID))
	for id := range r.outboxByID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	result := make([]OutboxEvent, 0, limit)
	for _, id := range ids {
		event := r.outboxByID[id]
		if event.Status != "pending" && event.Status != "retry" {
			continue
		}
		if event.NextAttempt.After(now) {
			continue
		}
		event.Status = "processing"
		r.outboxByID[id] = event
		result = append(result, event)
		if len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (r *InMemoryCaseRepository) MarkOutboxPublished(_ context.Context, outboxID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	event, ok := r.outboxByID[outboxID]
	if !ok {
		return sql.ErrNoRows
	}
	event.Status = "published"
	r.outboxByID[outboxID] = event
	return nil
}

func (r *InMemoryCaseRepository) MarkOutboxRetry(_ context.Context, outboxID int64, attempts int, nextAttempt time.Time, lastError string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	event, ok := r.outboxByID[outboxID]
	if !ok {
		return sql.ErrNoRows
	}
	event.Status = "retry"
	event.Attempts = attempts
	event.NextAttempt = nextAttempt
	event.LastError = lastError
	r.outboxByID[outboxID] = event
	return nil
}

func (r *InMemoryCaseRepository) MarkOutboxDead(_ context.Context, outboxID int64, attempts int, lastError string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	event, ok := r.outboxByID[outboxID]
	if !ok {
		return sql.ErrNoRows
	}
	event.Status = "dead"
	event.Attempts = attempts
	event.LastError = lastError
	r.outboxByID[outboxID] = event
	return nil
}

func (r *InMemoryCaseRepository) GetOpsMetrics(_ context.Context) (OpsMetrics, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var outboxPending int64
	var outboxDead int64
	for _, event := range r.outboxByID {
		if event.Status == "pending" || event.Status == "retry" || event.Status == "processing" {
			outboxPending++
		}
		if event.Status == "dead" {
			outboxDead++
		}
	}

	return OpsMetrics{
		TotalCases:        int64(len(r.cases)),
		IdempotentReplays: r.idempotentReplays,
		OutboxPending:     outboxPending,
		OutboxDeadLetter:  outboxDead,
		AuditLogCount:     r.auditLogCount,
	}, nil
}

func (r *InMemoryCaseRepository) appendAuditLog(caseID string, entry AuditLog) {
	r.auditLogsByCase[caseID] = append(r.auditLogsByCase[caseID], entry)
	r.auditLogCount++
}

type MySQLRedisCaseRepository struct {
	db       *sql.DB
	cache    redis.Cmdable
	cacheTTL time.Duration
}

func NewMySQLRedisCaseRepository(db *sql.DB, cache redis.Cmdable, cacheTTL time.Duration) *MySQLRedisCaseRepository {
	return &MySQLRedisCaseRepository{
		db:       db,
		cache:    cache,
		cacheTTL: cacheTTL,
	}
}

func (r *MySQLRedisCaseRepository) SaveCase(ctx context.Context, c Case) error {
	riskSignalsJSON, err := json.Marshal(c.RiskSignals)
	if err != nil {
		return err
	}
	evidenceJSON, err := json.Marshal(c.Evidence)
	if err != nil {
		return err
	}

	const upsert = `
INSERT INTO content_cases
	(case_id, status, content_id, content_type, risk_signals, evidence)
VALUES
	(?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
	status = VALUES(status),
	content_id = VALUES(content_id),
	content_type = VALUES(content_type),
	risk_signals = VALUES(risk_signals),
	evidence = VALUES(evidence),
	updated_at = CURRENT_TIMESTAMP
`
	if _, err := r.db.ExecContext(
		ctx,
		upsert,
		c.CaseID,
		c.Status,
		c.ContentID,
		c.ContentType,
		string(riskSignalsJSON),
		string(evidenceJSON),
	); err != nil {
		return err
	}

	r.cacheCase(ctx, c)
	return nil
}

func (r *MySQLRedisCaseRepository) GetCase(ctx context.Context, caseID string) (Case, bool, error) {
	if r.cache != nil {
		raw, err := r.cache.Get(ctx, cacheKey(caseID)).Result()
		if err == nil {
			var cached Case
			if unmarshalErr := json.Unmarshal([]byte(raw), &cached); unmarshalErr == nil {
				return cached, true, nil
			}
		}
	}

	record, found, err := queryCaseByID(ctx, r.db, caseID)
	if err != nil {
		return Case{}, false, err
	}
	if !found {
		return Case{}, false, nil
	}

	r.cacheCase(ctx, record)
	return record, true, nil
}

func (r *MySQLRedisCaseRepository) IngestCase(ctx context.Context, input IngestCaseInput) (IngestCaseResult, error) {
	key := normalizeIdempotencyKey(input)
	if key == "" {
		return IngestCaseResult{}, errors.New("idempotency_key_required")
	}

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return IngestCaseResult{}, err
	}

	var existingCaseID string
	err = tx.QueryRowContext(
		ctx,
		"SELECT case_id FROM content_ingest_idempotency WHERE idempotency_key = ?",
		key,
	).Scan(&existingCaseID)
	if err == nil {
		caseRecord, found, loadErr := queryCaseByID(ctx, tx, existingCaseID)
		if loadErr != nil {
			_ = tx.Rollback()
			return IngestCaseResult{}, loadErr
		}
		if !found {
			_ = tx.Rollback()
			return IngestCaseResult{}, errors.New("idempotency_case_missing")
		}
		if insertErr := insertAuditLog(ctx, tx, input.EventID, existingCaseID, "idempotent_replay", "duplicate event replayed"); insertErr != nil {
			_ = tx.Rollback()
			return IngestCaseResult{}, insertErr
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return IngestCaseResult{}, commitErr
		}
		r.cacheCase(ctx, caseRecord)
		return IngestCaseResult{
			Case:             caseRecord,
			IdempotentReplay: true,
		}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return IngestCaseResult{}, err
	}

	caseID := "case-" + input.EventID
	caseRecord := Case{
		CaseID:      caseID,
		Status:      "queued_for_review",
		ContentID:   input.ContentID,
		ContentType: input.ContentType,
		RiskSignals: append([]string(nil), input.RiskSignals...),
		Evidence:    append([]EvidenceItem(nil), input.Evidence...),
	}

	riskSignalsJSON, err := json.Marshal(caseRecord.RiskSignals)
	if err != nil {
		_ = tx.Rollback()
		return IngestCaseResult{}, err
	}
	evidenceJSON, err := json.Marshal(caseRecord.Evidence)
	if err != nil {
		_ = tx.Rollback()
		return IngestCaseResult{}, err
	}

	const caseUpsert = `
INSERT INTO content_cases
	(case_id, status, content_id, content_type, risk_signals, evidence)
VALUES
	(?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
	status = VALUES(status),
	content_id = VALUES(content_id),
	content_type = VALUES(content_type),
	risk_signals = VALUES(risk_signals),
	evidence = VALUES(evidence),
	updated_at = CURRENT_TIMESTAMP
`
	if _, err := tx.ExecContext(
		ctx,
		caseUpsert,
		caseRecord.CaseID,
		caseRecord.Status,
		caseRecord.ContentID,
		caseRecord.ContentType,
		string(riskSignalsJSON),
		string(evidenceJSON),
	); err != nil {
		_ = tx.Rollback()
		return IngestCaseResult{}, err
	}

	if _, err := tx.ExecContext(
		ctx,
		"INSERT INTO content_ingest_idempotency (idempotency_key, event_id, case_id) VALUES (?, ?, ?)",
		key,
		input.EventID,
		caseID,
	); err != nil {
		_ = tx.Rollback()
		if strings.Contains(strings.ToLower(err.Error()), "duplicate") {
			record, found, loadErr := queryCaseByID(ctx, r.db, caseID)
			if loadErr != nil {
				return IngestCaseResult{}, loadErr
			}
			if !found {
				return IngestCaseResult{}, err
			}
			return IngestCaseResult{
				Case:             record,
				IdempotentReplay: true,
			}, nil
		}
		return IngestCaseResult{}, err
	}

	payloadRaw, err := json.Marshal(map[string]any{
		"event_id":     input.EventID,
		"case_id":      caseID,
		"content_id":   input.ContentID,
		"content_type": input.ContentType,
		"risk_signals": input.RiskSignals,
	})
	if err != nil {
		_ = tx.Rollback()
		return IngestCaseResult{}, err
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO content_case_outbox
			(event_id, case_id, content_id, content_type, risk_signals, payload, status, attempts, next_attempt_at, last_error)
		VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, UTC_TIMESTAMP(), '')`,
		input.EventID,
		caseID,
		input.ContentID,
		input.ContentType,
		string(riskSignalsJSON),
		string(payloadRaw),
	); err != nil {
		_ = tx.Rollback()
		return IngestCaseResult{}, err
	}

	if err := insertAuditLog(ctx, tx, input.EventID, caseID, "ingest_accepted", "new ingest accepted and queued"); err != nil {
		_ = tx.Rollback()
		return IngestCaseResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return IngestCaseResult{}, err
	}

	r.cacheCase(ctx, caseRecord)
	return IngestCaseResult{
		Case:             caseRecord,
		IdempotentReplay: false,
	}, nil
}

func (r *MySQLRedisCaseRepository) ListAuditLogs(ctx context.Context, caseID string, limit int) ([]AuditLog, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	rows, err := r.db.QueryContext(
		ctx,
		"SELECT event_id, case_id, action, detail, created_at FROM content_audit_logs WHERE case_id = ? ORDER BY id DESC LIMIT ?",
		caseID,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	logs := make([]AuditLog, 0, limit)
	for rows.Next() {
		var log AuditLog
		if err := rows.Scan(&log.EventID, &log.CaseID, &log.Action, &log.Detail, &log.CreatedAt); err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return logs, nil
}

func (r *MySQLRedisCaseRepository) ClaimPendingOutboxEvents(ctx context.Context, limit int, now time.Time) ([]OutboxEvent, error) {
	if limit <= 0 {
		limit = 1
	}

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}

	rows, err := tx.QueryContext(
		ctx,
		`SELECT id, event_id, case_id, content_id, content_type, risk_signals, attempts, status, next_attempt_at, last_error
		 FROM content_case_outbox
		 WHERE status IN ('pending', 'retry') AND next_attempt_at <= ?
		 ORDER BY id ASC
		 LIMIT ?
		 FOR UPDATE`,
		now.UTC(),
		limit,
	)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	defer rows.Close()

	events := make([]OutboxEvent, 0, limit)
	for rows.Next() {
		var event OutboxEvent
		var riskSignalsRaw string
		if scanErr := rows.Scan(
			&event.ID,
			&event.EventID,
			&event.CaseID,
			&event.ContentID,
			&event.ContentType,
			&riskSignalsRaw,
			&event.Attempts,
			&event.Status,
			&event.NextAttempt,
			&event.LastError,
		); scanErr != nil {
			_ = tx.Rollback()
			return nil, scanErr
		}
		if unmarshalErr := json.Unmarshal([]byte(riskSignalsRaw), &event.RiskSignals); unmarshalErr != nil {
			_ = tx.Rollback()
			return nil, unmarshalErr
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	for _, event := range events {
		if _, err := tx.ExecContext(
			ctx,
			"UPDATE content_case_outbox SET status='processing', updated_at=UTC_TIMESTAMP() WHERE id=?",
			event.ID,
		); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return events, nil
}

func (r *MySQLRedisCaseRepository) MarkOutboxPublished(ctx context.Context, outboxID int64) error {
	_, err := r.db.ExecContext(
		ctx,
		"UPDATE content_case_outbox SET status='published', published_at=UTC_TIMESTAMP(), updated_at=UTC_TIMESTAMP() WHERE id=?",
		outboxID,
	)
	return err
}

func (r *MySQLRedisCaseRepository) MarkOutboxRetry(ctx context.Context, outboxID int64, attempts int, nextAttempt time.Time, lastError string) error {
	_, err := r.db.ExecContext(
		ctx,
		`UPDATE content_case_outbox
		 SET status='retry', attempts=?, next_attempt_at=?, last_error=?, updated_at=UTC_TIMESTAMP()
		 WHERE id=?`,
		attempts,
		nextAttempt.UTC(),
		truncateMessage(lastError, 1024),
		outboxID,
	)
	return err
}

func (r *MySQLRedisCaseRepository) MarkOutboxDead(ctx context.Context, outboxID int64, attempts int, lastError string) error {
	_, err := r.db.ExecContext(
		ctx,
		`UPDATE content_case_outbox
		 SET status='dead', attempts=?, last_error=?, updated_at=UTC_TIMESTAMP()
		 WHERE id=?`,
		attempts,
		truncateMessage(lastError, 1024),
		outboxID,
	)
	return err
}

func (r *MySQLRedisCaseRepository) GetOpsMetrics(ctx context.Context) (OpsMetrics, error) {
	metrics := OpsMetrics{}

	const totalCasesSQL = "SELECT COUNT(1) FROM content_cases"
	if err := r.db.QueryRowContext(ctx, totalCasesSQL).Scan(&metrics.TotalCases); err != nil {
		return OpsMetrics{}, err
	}

	const replaySQL = "SELECT COUNT(1) FROM content_audit_logs WHERE action='idempotent_replay'"
	if err := r.db.QueryRowContext(ctx, replaySQL).Scan(&metrics.IdempotentReplays); err != nil {
		return OpsMetrics{}, err
	}

	const pendingSQL = "SELECT COUNT(1) FROM content_case_outbox WHERE status IN ('pending', 'retry', 'processing')"
	if err := r.db.QueryRowContext(ctx, pendingSQL).Scan(&metrics.OutboxPending); err != nil {
		return OpsMetrics{}, err
	}

	const deadSQL = "SELECT COUNT(1) FROM content_case_outbox WHERE status='dead'"
	if err := r.db.QueryRowContext(ctx, deadSQL).Scan(&metrics.OutboxDeadLetter); err != nil {
		return OpsMetrics{}, err
	}

	const auditSQL = "SELECT COUNT(1) FROM content_audit_logs"
	if err := r.db.QueryRowContext(ctx, auditSQL).Scan(&metrics.AuditLogCount); err != nil {
		return OpsMetrics{}, err
	}

	return metrics, nil
}

func (r *MySQLRedisCaseRepository) cacheCase(ctx context.Context, c Case) {
	if r.cache == nil {
		return
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return
	}
	_ = r.cache.Set(ctx, cacheKey(c.CaseID), raw, r.cacheTTL).Err()
}

func cacheKey(caseID string) string {
	return "cq:case:" + caseID
}

func DefaultSeedCases() []Case {
	return []Case{
		{
			CaseID:      "case-001",
			Status:      "open",
			ContentID:   "content-seed-001",
			ContentType: "video",
			RiskSignals: []string{"low_quality"},
			Evidence: []EvidenceItem{
				{Type: "rule_hit", Detail: "rule:seed_low_quality"},
			},
		},
	}
}

type caseScanner interface {
	Scan(dest ...any) error
}

type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func queryCaseByID(ctx context.Context, db queryRower, caseID string) (Case, bool, error) {
	const selectByID = "SELECT case_id, status, content_id, content_type, risk_signals, evidence FROM content_cases WHERE case_id = ?"
	var record Case
	var riskSignalsRaw string
	var evidenceRaw string
	err := db.QueryRowContext(ctx, selectByID, caseID).Scan(
		&record.CaseID,
		&record.Status,
		&record.ContentID,
		&record.ContentType,
		&riskSignalsRaw,
		&evidenceRaw,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Case{}, false, nil
	}
	if err != nil {
		return Case{}, false, err
	}
	if err := json.Unmarshal([]byte(riskSignalsRaw), &record.RiskSignals); err != nil {
		return Case{}, false, err
	}
	if err := json.Unmarshal([]byte(evidenceRaw), &record.Evidence); err != nil {
		return Case{}, false, err
	}
	return record, true, nil
}

func insertAuditLog(ctx context.Context, tx *sql.Tx, eventID, caseID, action, detail string) error {
	_, err := tx.ExecContext(
		ctx,
		"INSERT INTO content_audit_logs (event_id, case_id, action, detail) VALUES (?, ?, ?, ?)",
		eventID,
		caseID,
		action,
		truncateMessage(detail, 1024),
	)
	return err
}

func normalizeIdempotencyKey(input IngestCaseInput) string {
	if strings.TrimSpace(input.IdempotencyKey) != "" {
		return strings.TrimSpace(input.IdempotencyKey)
	}
	return strings.TrimSpace(input.EventID)
}

func truncateMessage(v string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(v) <= max {
		return v
	}
	return fmt.Sprintf("%s...", v[:max-3])
}
