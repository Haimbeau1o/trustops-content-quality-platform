package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

type CaseRepository interface {
	SaveCase(ctx context.Context, c Case) error
	GetCase(ctx context.Context, caseID string) (Case, bool, error)
}

type InMemoryCaseRepository struct {
	mu    sync.RWMutex
	cases map[string]Case
}

func NewInMemoryCaseRepository(seed []Case) *InMemoryCaseRepository {
	m := map[string]Case{}
	for _, c := range seed {
		m[c.CaseID] = c
	}
	return &InMemoryCaseRepository{cases: m}
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

	const selectByID = "SELECT case_id, status, content_id, content_type, risk_signals, evidence FROM content_cases WHERE case_id = ?"
	var record Case
	var riskSignalsRaw string
	var evidenceRaw string
	err := r.db.QueryRowContext(ctx, selectByID, caseID).Scan(
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

	r.cacheCase(ctx, record)
	return record, true, nil
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
