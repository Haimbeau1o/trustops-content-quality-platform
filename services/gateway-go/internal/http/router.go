package http

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/mq"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/storage"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/utils"
)

type ingestEventRequest struct {
	EventID     string                 `json:"event_id"`
	ContentID   string                 `json:"content_id"`
	ContentType string                 `json:"content_type"`
	RiskSignals []string               `json:"risk_signals"`
	Evidence    []storage.EvidenceItem `json:"evidence"`
}

type Dependencies struct {
	CaseRepository storage.CaseRepository
	Publisher      mq.Publisher
}

type apiHandler struct {
	repo      storage.CaseRepository
	publisher mq.Publisher
}

func NewRouter() *server.Hertz {
	return NewRouterWithDependencies(Dependencies{})
}

func NewRouterWithDependencies(deps Dependencies) *server.Hertz {
	h := server.Default()
	RegisterRoutes(h, deps)
	return h
}

func RegisterRoutes(h *server.Hertz, deps Dependencies) {
	resolved := withDefaults(deps)
	handler := &apiHandler{
		repo:      resolved.CaseRepository,
		publisher: resolved.Publisher,
	}

	h.GET("/healthz", handler.healthzHandler)
	h.POST("/api/v1/content/events/ingest", handler.ingestContentEventHandler)
	h.GET("/api/v1/content/cases/:case_id", handler.getCaseByIDHandler)
}

func withDefaults(deps Dependencies) Dependencies {
	if deps.CaseRepository == nil {
		deps.CaseRepository = storage.NewInMemoryCaseRepository(storage.DefaultSeedCases())
	}
	if deps.Publisher == nil {
		deps.Publisher = mq.NewNoopPublisher()
	}
	return deps
}

func (h *apiHandler) healthzHandler(_ context.Context, c *app.RequestContext) {
	c.JSON(http.StatusOK, utils.H{
		"service": "content-quality-gateway",
		"status":  "ok",
	})
}

func (h *apiHandler) ingestContentEventHandler(ctx context.Context, c *app.RequestContext) {
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
	record := storage.Case{
		CaseID:      newCaseID,
		Status:      "queued_for_review",
		ContentID:   req.ContentID,
		ContentType: req.ContentType,
		RiskSignals: req.RiskSignals,
		Evidence:    req.Evidence,
	}

	if err := h.repo.SaveCase(ctx, record); err != nil {
		c.JSON(http.StatusInternalServerError, utils.H{"error": "case_persist_failed"})
		return
	}

	if err := h.publisher.PublishCaseIngested(ctx, mq.CaseEvent{
		EventID:     req.EventID,
		CaseID:      newCaseID,
		ContentID:   req.ContentID,
		ContentType: req.ContentType,
		RiskSignals: req.RiskSignals,
	}); err != nil {
		log.Printf("publish case event failed: %v", err)
	}

	c.JSON(http.StatusAccepted, utils.H{
		"case_id":     newCaseID,
		"status":      "queued_for_review",
		"next_action": "operator_triage",
	})
}

func (h *apiHandler) getCaseByIDHandler(ctx context.Context, c *app.RequestContext) {
	caseID := c.Param("case_id")
	record, ok, err := h.repo.GetCase(ctx, caseID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.H{"error": "case_lookup_failed"})
		return
	}

	if !ok {
		c.JSON(http.StatusNotFound, utils.H{
			"error":   "case_not_found",
			"case_id": caseID,
		})
		return
	}

	c.JSON(http.StatusOK, record)
}
