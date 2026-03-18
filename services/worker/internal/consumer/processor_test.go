package consumer

import (
	"log"
	"testing"
)

func TestParseCaseEvent(t *testing.T) {
	event, err := ParseCaseEvent([]byte(`{"event_id":"evt-1","case_id":"case-1","content_id":"content-1","content_type":"video"}`))
	if err != nil {
		t.Fatalf("ParseCaseEvent() error = %v", err)
	}
	if event.EventID != "evt-1" || event.CaseID != "case-1" {
		t.Fatalf("unexpected parsed event: %#v", event)
	}
}

func TestParseCaseEventInvalid(t *testing.T) {
	_, err := ParseCaseEvent([]byte(`{"event_id":"","case_id":"case-1"}`))
	if err == nil {
		t.Fatalf("expected parse validation error")
	}
}

func TestProcessEvent(t *testing.T) {
	logger := log.Default()
	err := ProcessEvent(logger, CaseEvent{
		EventID:     "evt-2",
		CaseID:      "case-2",
		ContentID:   "content-2",
		ContentType: "image",
	})
	if err != nil {
		t.Fatalf("ProcessEvent() error = %v", err)
	}
}
