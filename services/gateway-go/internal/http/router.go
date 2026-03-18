package http

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/utils"
)

type evidenceItem struct {
	Type   string `json:"type"`
	Detail string `json:"detail"`
}

type ingestEventRequest struct {
	EventID     string         `json:"event_id"`
	ContentID   string         `json:"content_id"`
	ContentType string         `json:"content_type"`
	RiskSignals []string       `json:"risk_signals"`
	Evidence    []evidenceItem `json:"evidence"`
}

type caseRecord struct {
	CaseID      string         `json:"case_id"`
	Status      string         `json:"status"`
	ContentID   string         `json:"content_id"`
	ContentType string         `json:"content_type"`
	RiskSignals []string       `json:"risk_signals"`
	Evidence    []evidenceItem `json:"evidence"`
}

var (
	caseStoreMu sync.RWMutex
	caseStore   map[string]caseRecord
)

func NewRouter() *server.Hertz {
	h := server.Default()
	RegisterRoutes(h)
	return h
}

func RegisterRoutes(h *server.Hertz) {
	caseStoreMu.Lock()
	caseStore = map[string]caseRecord{
		"case-001": {
			CaseID:      "case-001",
			Status:      "open",
			ContentID:   "content-seed-001",
			ContentType: "video",
			RiskSignals: []string{"low_quality"},
			Evidence: []evidenceItem{
				{Type: "rule_hit", Detail: "rule:seed_low_quality"},
			},
		},
	}
	caseStoreMu.Unlock()

	h.GET("/healthz", healthzHandler)
	h.POST("/api/v1/content/events/ingest", ingestContentEventHandler)
	h.GET("/api/v1/content/cases/:case_id", getCaseByIDHandler)
}

func healthzHandler(_ context.Context, c *app.RequestContext) {
	c.JSON(http.StatusOK, utils.H{
		"service": "content-quality-gateway",
		"status":  "ok",
	})
}

func ingestContentEventHandler(_ context.Context, c *app.RequestContext) {
	var req ingestEventRequest
	if err := json.Unmarshal(c.Request.Body(), &req); err != nil {
		c.JSON(http.StatusBadRequest, utils.H{"error": "invalid_json"})
		return
	}
	if req.EventID == "" {
		c.JSON(http.StatusBadRequest, utils.H{"error": "event_id_required"})
		return
	}

	newCaseID := "case-" + req.EventID
	record := caseRecord{
		CaseID:      newCaseID,
		Status:      "queued_for_review",
		ContentID:   req.ContentID,
		ContentType: req.ContentType,
		RiskSignals: req.RiskSignals,
		Evidence:    req.Evidence,
	}

	caseStoreMu.Lock()
	caseStore[newCaseID] = record
	caseStoreMu.Unlock()

	c.JSON(http.StatusAccepted, utils.H{
		"case_id":     newCaseID,
		"status":      "queued_for_review",
		"next_action": "operator_triage",
	})
}

func getCaseByIDHandler(_ context.Context, c *app.RequestContext) {
	caseID := c.Param("case_id")

	caseStoreMu.RLock()
	record, ok := caseStore[caseID]
	caseStoreMu.RUnlock()

	if !ok {
		c.JSON(http.StatusNotFound, utils.H{
			"error":   "case_not_found",
			"case_id": caseID,
		})
		return
	}

	c.JSON(http.StatusOK, record)
}
