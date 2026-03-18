package http

import (
	"context"
	"strings"
	"testing"

	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/mq"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/storage"
	"github.com/cloudwego/hertz/pkg/common/test/assert"
	"github.com/cloudwego/hertz/pkg/common/ut"
)

type fakeCaseRepository struct {
	saved map[string]storage.Case
}

func newFakeCaseRepository() *fakeCaseRepository {
	return &fakeCaseRepository{saved: map[string]storage.Case{}}
}

func (f *fakeCaseRepository) SaveCase(_ context.Context, c storage.Case) error {
	f.saved[c.CaseID] = c
	return nil
}

func (f *fakeCaseRepository) GetCase(_ context.Context, caseID string) (storage.Case, bool, error) {
	c, ok := f.saved[caseID]
	return c, ok, nil
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
	pub := &fakePublisher{}
	h := NewRouterWithDependencies(Dependencies{
		CaseRepository: repo,
		Publisher:      pub,
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

	assert.DeepEqual(t, 1, len(pub.events))
	assert.DeepEqual(t, "evt-3001", pub.events[0].EventID)
	assert.DeepEqual(t, "case-evt-3001", pub.events[0].CaseID)
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
