package consumer

import (
	"encoding/json"
	"errors"
	"log"
)

var ErrInvalidCaseEvent = errors.New("invalid_case_event")

type CaseEvent struct {
	EventID     string `json:"event_id"`
	CaseID      string `json:"case_id"`
	ContentID   string `json:"content_id,omitempty"`
	ContentType string `json:"content_type,omitempty"`
}

func ParseCaseEvent(body []byte) (CaseEvent, error) {
	var event CaseEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return CaseEvent{}, err
	}
	if event.EventID == "" || event.CaseID == "" {
		return CaseEvent{}, ErrInvalidCaseEvent
	}
	return event, nil
}

func ProcessEvent(logger *log.Logger, event CaseEvent) error {
	logger.Printf(
		"processed case event event_id=%s case_id=%s content_id=%s content_type=%s",
		event.EventID,
		event.CaseID,
		event.ContentID,
		event.ContentType,
	)
	return nil
}
