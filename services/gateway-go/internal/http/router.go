package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/mq"
	"github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/security"
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
	Authorizer     *security.APIKeyAuthorizer
	RateLimiter    security.RateLimiter
}

type apiHandler struct {
	repo        storage.CaseRepository
	publisher   mq.Publisher
	authorizer  *security.APIKeyAuthorizer
	rateLimiter security.RateLimiter
	authRejects atomic.Int64
	rateRejects atomic.Int64
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
		repo:        resolved.CaseRepository,
		publisher:   resolved.Publisher,
		authorizer:  resolved.Authorizer,
		rateLimiter: resolved.RateLimiter,
	}

	h.GET("/healthz", handler.healthzHandler)
	h.POST("/api/v1/content/events/ingest", handler.ingestContentEventHandler)
	h.GET("/api/v1/content/cases/:case_id", handler.getCaseByIDHandler)
	h.GET("/api/v1/content/cases/:case_id/audit-logs", handler.getCaseAuditLogsHandler)
	h.GET("/api/v1/ops/metrics", handler.opsMetricsHandler)
	h.GET("/metrics", handler.prometheusMetricsHandler)
}

func withDefaults(deps Dependencies) Dependencies {
	if deps.CaseRepository == nil {
		deps.CaseRepository = storage.NewInMemoryCaseRepository(storage.DefaultSeedCases())
	}
	if deps.Publisher == nil {
		deps.Publisher = mq.NewNoopPublisher()
	}
	if deps.Authorizer == nil {
		deps.Authorizer = security.NewAPIKeyAuthorizer(nil)
	}
	if deps.RateLimiter == nil {
		deps.RateLimiter = security.NewNoopLimiter()
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
	if !h.authorizeAndRateLimit(ctx, c) {
		return
	}

	var req ingestEventRequest
	if err := json.Unmarshal(c.Request.Body(), &req); err != nil {
		c.JSON(http.StatusBadRequest, utils.H{"error": "invalid_json"})
		return
	}
	if req.EventID == "" {
		c.JSON(http.StatusBadRequest, utils.H{"error": "event_id_required"})
		return
	}

	result, err := h.repo.IngestCase(ctx, storage.IngestCaseInput{
		IdempotencyKey: req.EventID,
		EventID:        req.EventID,
		ContentID:      req.ContentID,
		ContentType:    req.ContentType,
		RiskSignals:    req.RiskSignals,
		Evidence:       req.Evidence,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.H{"error": "case_persist_failed"})
		return
	}

	c.JSON(http.StatusAccepted, utils.H{
		"case_id":            result.Case.CaseID,
		"status":             result.Case.Status,
		"next_action":        "operator_triage",
		"idempotent_replay":  result.IdempotentReplay,
		"event_publish_mode": "outbox_relay",
	})
}

func (h *apiHandler) getCaseByIDHandler(ctx context.Context, c *app.RequestContext) {
	if !h.authorizeAndRateLimit(ctx, c) {
		return
	}

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

func (h *apiHandler) opsMetricsHandler(ctx context.Context, c *app.RequestContext) {
	if !h.authorizeAndRateLimit(ctx, c) {
		return
	}

	metrics, err := h.repo.GetOpsMetrics(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.H{"error": "ops_metrics_failed"})
		return
	}
	c.JSON(http.StatusOK, metrics)
}

func (h *apiHandler) getCaseAuditLogsHandler(ctx context.Context, c *app.RequestContext) {
	if !h.authorizeAndRateLimit(ctx, c) {
		return
	}

	caseID := c.Param("case_id")
	limit := 20
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > 200 {
		limit = 200
	}

	logs, err := h.repo.ListAuditLogs(ctx, caseID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.H{"error": "audit_logs_query_failed"})
		return
	}

	c.JSON(http.StatusOK, utils.H{
		"case_id": caseID,
		"logs":    logs,
	})
}

func (h *apiHandler) prometheusMetricsHandler(ctx context.Context, c *app.RequestContext) {
	if !h.authorizeAndRateLimit(ctx, c) {
		return
	}

	ops, err := h.repo.GetOpsMetrics(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.H{"error": "ops_metrics_failed"})
		return
	}

	text := fmt.Sprintf(
		`# HELP trustops_content_total_cases Total number of content cases.
# TYPE trustops_content_total_cases gauge
trustops_content_total_cases %d
# HELP trustops_content_idempotent_replays_total Total idempotent replay ingests.
# TYPE trustops_content_idempotent_replays_total counter
trustops_content_idempotent_replays_total %d
# HELP trustops_content_outbox_pending_total Total outbox rows pending dispatch.
# TYPE trustops_content_outbox_pending_total gauge
trustops_content_outbox_pending_total %d
# HELP trustops_content_outbox_dead_letter_total Total outbox rows in dead-letter.
# TYPE trustops_content_outbox_dead_letter_total gauge
trustops_content_outbox_dead_letter_total %d
# HELP trustops_content_audit_log_total Total persisted audit log rows.
# TYPE trustops_content_audit_log_total gauge
trustops_content_audit_log_total %d
# HELP trustops_gateway_auth_reject_total Total authentication rejects.
# TYPE trustops_gateway_auth_reject_total counter
trustops_gateway_auth_reject_total %d
# HELP trustops_gateway_rate_limit_reject_total Total rate-limit rejects.
# TYPE trustops_gateway_rate_limit_reject_total counter
trustops_gateway_rate_limit_reject_total %d
`,
		ops.TotalCases,
		ops.IdempotentReplays,
		ops.OutboxPending,
		ops.OutboxDeadLetter,
		ops.AuditLogCount,
		h.authRejects.Load(),
		h.rateRejects.Load(),
	)
	c.Header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	c.String(http.StatusOK, text)
}

func (h *apiHandler) authorizeAndRateLimit(ctx context.Context, c *app.RequestContext) bool {
	apiKey := strings.TrimSpace(string(c.Request.Header.Peek("X-API-Key")))
	switch h.authorizer.Authorize(apiKey) {
	case security.AuthMissing:
		h.authRejects.Add(1)
		c.JSON(http.StatusUnauthorized, utils.H{"error": "api_key_required"})
		return false
	case security.AuthInvalid:
		h.authRejects.Add(1)
		c.JSON(http.StatusForbidden, utils.H{"error": "api_key_invalid"})
		return false
	}

	allowed, err := h.rateLimiter.Allow(ctx, security.BuildLimiterKey(apiKey, string(c.Path())))
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.H{"error": "rate_limiter_failed"})
		return false
	}
	if !allowed {
		h.rateRejects.Add(1)
		c.JSON(http.StatusTooManyRequests, utils.H{"error": "rate_limited"})
		return false
	}
	return true
}
