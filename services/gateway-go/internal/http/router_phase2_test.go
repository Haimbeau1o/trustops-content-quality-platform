package http

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/mq"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/security"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/storage"
	"github.com/cloudwego/hertz/pkg/common/test/assert"
	"github.com/cloudwego/hertz/pkg/common/ut"
)

type fakeCaseRepository struct {
	saved           map[string]storage.Case
	ingestByEventID map[string]storage.IngestCaseResult
	opsMetrics      storage.OpsMetrics
	auditByCaseID   map[string][]storage.AuditLog
}

func newFakeCaseRepository() *fakeCaseRepository {
	return &fakeCaseRepository{
		saved:           map[string]storage.Case{},
		ingestByEventID: map[string]storage.IngestCaseResult{},
		auditByCaseID:   map[string][]storage.AuditLog{},
		opsMetrics: storage.OpsMetrics{
			TotalCases:        0,
			IdempotentReplays: 0,
			OutboxPending:     0,
			OutboxDeadLetter:  0,
			AuditLogCount:     0,
		},
	}
}

func (f *fakeCaseRepository) SaveCase(_ context.Context, c storage.Case) error {
	f.saved[c.CaseID] = c
	return nil
}

func (f *fakeCaseRepository) GetCase(_ context.Context, caseID string) (storage.Case, bool, error) {
	c, ok := f.saved[caseID]
	return c, ok, nil
}

func (f *fakeCaseRepository) IngestCase(_ context.Context, input storage.IngestCaseInput) (storage.IngestCaseResult, error) {
	if existing, ok := f.ingestByEventID[input.EventID]; ok {
		f.opsMetrics.IdempotentReplays++
		f.opsMetrics.AuditLogCount++
		existing.IdempotentReplay = true
		f.ingestByEventID[input.EventID] = existing
		return existing, nil
	}

	newCase := storage.Case{
		CaseID:      "case-" + input.EventID,
		Status:      "queued_for_review",
		ContentID:   input.ContentID,
		ContentType: input.ContentType,
		RiskSignals: input.RiskSignals,
		Evidence:    input.Evidence,
	}
	f.saved[newCase.CaseID] = newCase
	now := time.Now().UTC()
	f.auditByCaseID[newCase.CaseID] = append(f.auditByCaseID[newCase.CaseID], storage.AuditLog{
		EventID:   input.EventID,
		CaseID:    newCase.CaseID,
		Action:    "ingest_accepted",
		Detail:    "new ingest accepted and queued",
		CreatedAt: now,
	})
	result := storage.IngestCaseResult{
		Case:             newCase,
		IdempotentReplay: false,
	}
	f.ingestByEventID[input.EventID] = result
	f.opsMetrics.TotalCases++
	f.opsMetrics.OutboxPending++
	f.opsMetrics.AuditLogCount++
	return result, nil
}

func (f *fakeCaseRepository) ClaimPendingOutboxEvents(_ context.Context, _ int, _ time.Time) ([]storage.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeCaseRepository) MarkOutboxPublished(_ context.Context, _ int64) error {
	return nil
}

func (f *fakeCaseRepository) MarkOutboxRetry(_ context.Context, _ int64, _ int, _ time.Time, _ string) error {
	return nil
}

func (f *fakeCaseRepository) MarkOutboxDead(_ context.Context, _ int64, _ int, _ string) error {
	return nil
}

func (f *fakeCaseRepository) GetOpsMetrics(_ context.Context) (storage.OpsMetrics, error) {
	return f.opsMetrics, nil
}

func (f *fakeCaseRepository) ListAuditLogs(_ context.Context, caseID string, limit int) ([]storage.AuditLog, error) {
	logs := append([]storage.AuditLog(nil), f.auditByCaseID[caseID]...)
	if limit <= 0 || limit > len(logs) {
		limit = len(logs)
	}
	return logs[:limit], nil
}

type fakePublisher struct {
	events []mq.CaseEvent
}

func (f *fakePublisher) PublishCaseIngested(_ context.Context, event mq.CaseEvent) error {
	f.events = append(f.events, event)
	return nil
}

func (f *fakePublisher) Close() error {
	return nil
}

func TestIngestPersistsCaseAndPublishesEvent(t *testing.T) {
	repo := newFakeCaseRepository()
	h := NewRouterWithDependencies(Dependencies{
		CaseRepository: repo,
		Publisher:      &fakePublisher{},
		Authorizer:     security.NewAPIKeyAuthorizer([]string{"test-key"}),
		RateLimiter:    security.NewInMemoryFixedWindowLimiter(100, time.Minute),
	})

	body := `{
		"event_id":"evt-3001",
		"content_id":"content-3001",
		"content_type":"video",
		"risk_signals":["spam","unsafe"],
		"evidence":[{"type":"rule_hit","detail":"rule:unsafe"}]
	}`
	requestBody := &ut.Body{
		Body: strings.NewReader(body),
		Len:  len(body),
	}

	resp := ut.PerformRequest(
		h.Engine,
		"POST",
		"/api/v1/content/events/ingest",
		requestBody,
		ut.Header{Key: "Content-Type", Value: "application/json"},
		ut.Header{Key: "X-API-Key", Value: "test-key"},
	)

	assert.DeepEqual(t, 202, resp.Code)
	c, ok := repo.saved["case-evt-3001"]
	assert.True(t, ok)
	assert.DeepEqual(t, "queued_for_review", c.Status)
	assert.DeepEqual(t, "content-3001", c.ContentID)
}

func TestIngestEventSupportsIdempotencyReplay(t *testing.T) {
	repo := newFakeCaseRepository()
	h := NewRouterWithDependencies(Dependencies{
		CaseRepository: repo,
		Publisher:      &fakePublisher{},
		Authorizer:     security.NewAPIKeyAuthorizer([]string{"test-key"}),
		RateLimiter:    security.NewInMemoryFixedWindowLimiter(100, time.Minute),
	})

	body := `{
		"event_id":"evt-dup-3001",
		"content_id":"content-3001",
		"content_type":"video",
		"risk_signals":["spam","unsafe"],
		"evidence":[{"type":"rule_hit","detail":"rule:unsafe"}]
	}`
	requestBody := &ut.Body{
		Body: strings.NewReader(body),
		Len:  len(body),
	}
	first := ut.PerformRequest(
		h.Engine,
		"POST",
		"/api/v1/content/events/ingest",
		requestBody,
		ut.Header{Key: "Content-Type", Value: "application/json"},
		ut.Header{Key: "X-API-Key", Value: "test-key"},
	)
	assert.DeepEqual(t, 202, first.Code)

	secondBody := &ut.Body{
		Body: strings.NewReader(body),
		Len:  len(body),
	}
	second := ut.PerformRequest(
		h.Engine,
		"POST",
		"/api/v1/content/events/ingest",
		secondBody,
		ut.Header{Key: "Content-Type", Value: "application/json"},
		ut.Header{Key: "X-API-Key", Value: "test-key"},
	)
	assert.DeepEqual(t, 202, second.Code)

	var secondJSON map[string]any
	err := json.Unmarshal(second.Body.Bytes(), &secondJSON)
	assert.Nil(t, err)
	assert.DeepEqual(t, true, secondJSON["idempotent_replay"])
}

func TestOpsMetricsEndpoint(t *testing.T) {
	repo := newFakeCaseRepository()
	repo.opsMetrics = storage.OpsMetrics{
		TotalCases:        5,
		IdempotentReplays: 2,
		OutboxPending:     1,
		OutboxDeadLetter:  1,
		AuditLogCount:     8,
	}
	h := NewRouterWithDependencies(Dependencies{
		CaseRepository: repo,
		Publisher:      &fakePublisher{},
		Authorizer:     security.NewAPIKeyAuthorizer([]string{"test-key"}),
		RateLimiter:    security.NewInMemoryFixedWindowLimiter(100, time.Minute),
	})

	resp := ut.PerformRequest(
		h.Engine,
		"GET",
		"/api/v1/ops/metrics",
		nil,
		ut.Header{Key: "X-API-Key", Value: "test-key"},
	)
	assert.DeepEqual(t, 200, resp.Code)
	assert.True(t, strings.Contains(string(resp.Body.Bytes()), `"total_cases":5`))
	assert.True(t, strings.Contains(string(resp.Body.Bytes()), `"idempotent_replays":2`))
	assert.True(t, strings.Contains(string(resp.Body.Bytes()), `"outbox_pending":1`))
	assert.True(t, strings.Contains(string(resp.Body.Bytes()), `"outbox_dead_letter":1`))
}

func TestGetCaseReadsFromRepository(t *testing.T) {
	repo := newFakeCaseRepository()
	repo.saved["case-501"] = storage.Case{
		CaseID:      "case-501",
		Status:      "open",
		ContentID:   "content-501",
		ContentType: "image",
		RiskSignals: []string{"low_quality"},
		Evidence:    []storage.EvidenceItem{{Type: "rule_hit", Detail: "rule:blur"}},
	}

	h := NewRouterWithDependencies(Dependencies{
		CaseRepository: repo,
		Publisher:      &fakePublisher{},
		Authorizer:     security.NewAPIKeyAuthorizer([]string{"test-key"}),
		RateLimiter:    security.NewInMemoryFixedWindowLimiter(100, time.Minute),
	})

	resp := ut.PerformRequest(
		h.Engine,
		"GET",
		"/api/v1/content/cases/case-501",
		nil,
		ut.Header{Key: "X-API-Key", Value: "test-key"},
	)
	assert.DeepEqual(t, 200, resp.Code)
	assert.True(t, strings.Contains(string(resp.Body.Bytes()), `"case_id":"case-501"`))
}

func TestProtectedRoutesRequireAPIKey(t *testing.T) {
	repo := newFakeCaseRepository()
	h := NewRouterWithDependencies(Dependencies{
		CaseRepository: repo,
		Publisher:      &fakePublisher{},
		Authorizer:     security.NewAPIKeyAuthorizer([]string{"test-key"}),
		RateLimiter:    security.NewInMemoryFixedWindowLimiter(100, time.Minute),
	})

	resp := ut.PerformRequest(h.Engine, "GET", "/api/v1/content/cases/case-501", nil)
	assert.DeepEqual(t, 401, resp.Code)
	assert.True(t, strings.Contains(string(resp.Body.Bytes()), `"error":"api_key_required"`))
}

func TestProtectedRoutesRejectInvalidAPIKey(t *testing.T) {
	repo := newFakeCaseRepository()
	h := NewRouterWithDependencies(Dependencies{
		CaseRepository: repo,
		Publisher:      &fakePublisher{},
		Authorizer:     security.NewAPIKeyAuthorizer([]string{"valid-key"}),
		RateLimiter:    security.NewInMemoryFixedWindowLimiter(100, time.Minute),
	})

	resp := ut.PerformRequest(
		h.Engine,
		"GET",
		"/api/v1/content/cases/case-501",
		nil,
		ut.Header{Key: "X-API-Key", Value: "wrong-key"},
	)
	assert.DeepEqual(t, 403, resp.Code)
	assert.True(t, strings.Contains(string(resp.Body.Bytes()), `"error":"api_key_invalid"`))
}

func TestRateLimitRejectsExcessiveRequests(t *testing.T) {
	repo := newFakeCaseRepository()
	h := NewRouterWithDependencies(Dependencies{
		CaseRepository: repo,
		Publisher:      &fakePublisher{},
		Authorizer:     security.NewAPIKeyAuthorizer([]string{"test-key"}),
		RateLimiter:    security.NewInMemoryFixedWindowLimiter(1, time.Minute),
	})

	first := ut.PerformRequest(
		h.Engine,
		"GET",
		"/api/v1/content/cases/case-501",
		nil,
		ut.Header{Key: "X-API-Key", Value: "test-key"},
	)
	assert.DeepEqual(t, 404, first.Code)

	second := ut.PerformRequest(
		h.Engine,
		"GET",
		"/api/v1/content/cases/case-501",
		nil,
		ut.Header{Key: "X-API-Key", Value: "test-key"},
	)
	assert.DeepEqual(t, 429, second.Code)
	assert.True(t, strings.Contains(string(second.Body.Bytes()), `"error":"rate_limited"`))
}

func TestGetCaseAuditLogs(t *testing.T) {
	repo := newFakeCaseRepository()
	repo.auditByCaseID["case-777"] = []storage.AuditLog{
		{
			EventID:   "evt-2",
			CaseID:    "case-777",
			Action:    "relay_published",
			Detail:    "published",
			CreatedAt: time.Now().UTC(),
		},
		{
			EventID:   "evt-1",
			CaseID:    "case-777",
			Action:    "ingest_accepted",
			Detail:    "accepted",
			CreatedAt: time.Now().UTC().Add(-1 * time.Minute),
		},
	}
	h := NewRouterWithDependencies(Dependencies{
		CaseRepository: repo,
		Publisher:      &fakePublisher{},
		Authorizer:     security.NewAPIKeyAuthorizer([]string{"test-key"}),
		RateLimiter:    security.NewInMemoryFixedWindowLimiter(100, time.Minute),
	})

	resp := ut.PerformRequest(
		h.Engine,
		"GET",
		"/api/v1/content/cases/case-777/audit-logs?limit=1",
		nil,
		ut.Header{Key: "X-API-Key", Value: "test-key"},
	)
	assert.DeepEqual(t, 200, resp.Code)
	body := string(resp.Body.Bytes())
	assert.True(t, strings.Contains(body, `"case_id":"case-777"`))
	assert.True(t, strings.Contains(body, `"action":"relay_published"`))
	assert.True(t, !strings.Contains(body, `"action":"ingest_accepted"`))
}

func TestPrometheusMetricsEndpoint(t *testing.T) {
	repo := newFakeCaseRepository()
	repo.opsMetrics = storage.OpsMetrics{
		TotalCases:        3,
		IdempotentReplays: 1,
		OutboxPending:     2,
		OutboxDeadLetter:  1,
		AuditLogCount:     7,
	}
	h := NewRouterWithDependencies(Dependencies{
		CaseRepository: repo,
		Publisher:      &fakePublisher{},
		Authorizer:     security.NewAPIKeyAuthorizer([]string{"test-key"}),
		RateLimiter:    security.NewInMemoryFixedWindowLimiter(100, time.Minute),
	})

	_ = ut.PerformRequest(
		h.Engine,
		"GET",
		"/api/v1/content/cases/case-404",
		nil,
		ut.Header{Key: "X-API-Key", Value: "invalid-key"},
	)
	_ = ut.PerformRequest(
		h.Engine,
		"GET",
		"/api/v1/content/cases/case-404",
		nil,
		ut.Header{Key: "X-API-Key", Value: "test-key"},
	)
	_ = ut.PerformRequest(
		h.Engine,
		"GET",
		"/api/v1/content/cases/case-404",
		nil,
		ut.Header{Key: "X-API-Key", Value: "test-key"},
	)
	// one-key-per-minute limiter forces a rate limited call for runtime counter checks
	limitedRouter := NewRouterWithDependencies(Dependencies{
		CaseRepository: repo,
		Publisher:      &fakePublisher{},
		Authorizer:     security.NewAPIKeyAuthorizer([]string{"limited-key"}),
		RateLimiter:    security.NewInMemoryFixedWindowLimiter(1, time.Minute),
	})
	_ = ut.PerformRequest(
		limitedRouter.Engine,
		"GET",
		"/api/v1/content/cases/case-404",
		nil,
		ut.Header{Key: "X-API-Key", Value: "limited-key"},
	)
	_ = ut.PerformRequest(
		limitedRouter.Engine,
		"GET",
		"/api/v1/content/cases/case-404",
		nil,
		ut.Header{Key: "X-API-Key", Value: "limited-key"},
	)

	resp := ut.PerformRequest(
		h.Engine,
		"GET",
		"/metrics",
		nil,
		ut.Header{Key: "X-API-Key", Value: "test-key"},
	)
	assert.DeepEqual(t, 200, resp.Code)
	body := string(resp.Body.Bytes())
	assert.True(t, strings.Contains(body, "trustops_content_total_cases 3"))
	assert.True(t, strings.Contains(body, "trustops_content_idempotent_replays_total 1"))
	assert.True(t, strings.Contains(body, "trustops_content_outbox_pending_total 2"))
	assert.True(t, strings.Contains(body, "trustops_content_outbox_dead_letter_total 1"))
	assert.True(t, strings.Contains(body, "trustops_content_audit_log_total 7"))
	assert.True(t, strings.Contains(body, "trustops_gateway_auth_reject_total"))
	assert.True(t, strings.Contains(body, "trustops_gateway_rate_limit_reject_total"))
}
