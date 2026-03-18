package http

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/mq"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/storage"
	"github.com/cloudwego/hertz/pkg/common/test/assert"
	"github.com/cloudwego/hertz/pkg/common/ut"
)

type fakeCaseRepository struct {
	saved           map[string]storage.Case
	ingestByEventID map[string]storage.IngestCaseResult
	opsMetrics      storage.OpsMetrics
}

func newFakeCaseRepository() *fakeCaseRepository {
	return &fakeCaseRepository{
		saved:           map[string]storage.Case{},
		ingestByEventID: map[string]storage.IngestCaseResult{},
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
	})

	resp := ut.PerformRequest(h.Engine, "GET", "/api/v1/ops/metrics", nil)
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
	})

	resp := ut.PerformRequest(h.Engine, "GET", "/api/v1/content/cases/case-501", nil)
	assert.DeepEqual(t, 200, resp.Code)
	assert.True(t, strings.Contains(string(resp.Body.Bytes()), `"case_id":"case-501"`))
}
