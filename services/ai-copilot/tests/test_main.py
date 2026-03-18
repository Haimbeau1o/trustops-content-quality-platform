from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_healthz() -> None:
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"service": "content-quality-copilot", "status": "ok"}


def test_content_summary() -> None:
    payload = {
        "case_id": "case-evt-1001",
        "title": "Potential spam upload",
        "content_id": "content-9",
        "risk_signals": ["spam", "low_quality"],
        "evidence": [
            {"type": "rule_hit", "detail": "rule:spam_keyword"},
            {"type": "report_count", "detail": "reports:4"},
        ],
    }
    resp = client.post("/copilot/content/summary", json=payload)

    assert resp.status_code == 200
    body = resp.json()
    assert body["case_id"] == "case-evt-1001"
    assert body["decision_chain"] == "rules-first-ai-enhancement"
    assert body["evidence_count"] == 2
    assert "summary" in body
