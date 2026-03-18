package http

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/common/test/assert"
	"github.com/cloudwego/hertz/pkg/common/ut"
)

func TestHealthz(t *testing.T) {
	h := NewRouter()
	resp := ut.PerformRequest(h.Engine, "GET", "/healthz", nil)

	assert.DeepEqual(t, 200, resp.Code)
	var payload map[string]string
	err := json.Unmarshal(resp.Body.Bytes(), &payload)
	assert.Nil(t, err)
	assert.DeepEqual(t, "content-quality-gateway", payload["service"])
	assert.DeepEqual(t, "ok", payload["status"])
}

func TestIngestContentEvent(t *testing.T) {
	h := NewRouter()
	body := `{
		"event_id":"evt-1001",
		"content_id":"content-9",
		"content_type":"video",
		"risk_signals":["spam","low_quality"],
		"evidence":[
			{"type":"rule_hit","detail":"rule:spam_keyword"},
			{"type":"report_count","detail":"reports:4"}
		]
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
	got := string(resp.Body.Bytes())
	assert.True(t, strings.Contains(got, `"case_id":"case-evt-1001"`))
	assert.True(t, strings.Contains(got, `"status":"queued_for_review"`))
}

func TestGetCaseByIDFound(t *testing.T) {
	h := NewRouter()

	resp := ut.PerformRequest(h.Engine, "GET", "/api/v1/content/cases/case-001", nil)

	assert.DeepEqual(t, 200, resp.Code)
	got := string(resp.Body.Bytes())
	assert.True(t, strings.Contains(got, `"case_id":"case-001"`))
	assert.True(t, strings.Contains(got, `"status":"open"`))
	assert.True(t, strings.Contains(got, `"content_id":"content-seed-001"`))
}

func TestGetCaseByIDNotFound(t *testing.T) {
	h := NewRouter()

	resp := ut.PerformRequest(h.Engine, "GET", "/api/v1/content/cases/case-404", nil)

	assert.DeepEqual(t, 404, resp.Code)
	assert.True(t, strings.Contains(string(resp.Body.Bytes()), `"error":"case_not_found"`))
}
