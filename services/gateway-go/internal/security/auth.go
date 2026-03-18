package security

import "strings"

type AuthDecision string

const (
	AuthAllowed AuthDecision = "allowed"
	AuthMissing AuthDecision = "missing"
	AuthInvalid AuthDecision = "invalid"
)

type APIKeyAuthorizer struct {
	allowed map[string]struct{}
}

func NewAPIKeyAuthorizer(keys []string) *APIKeyAuthorizer {
	allowed := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		allowed[trimmed] = struct{}{}
	}
	return &APIKeyAuthorizer{allowed: allowed}
}

func (a *APIKeyAuthorizer) Authorize(rawKey string) AuthDecision {
	if len(a.allowed) == 0 {
		return AuthAllowed
	}

	key := strings.TrimSpace(rawKey)
	if key == "" {
		return AuthMissing
	}
	if _, ok := a.allowed[key]; ok {
		return AuthAllowed
	}
	return AuthInvalid
}
