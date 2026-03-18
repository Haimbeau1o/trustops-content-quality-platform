from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel, Field


class EvidenceItem(BaseModel):
    type: str
    detail: str


class ContentSummaryRequest(BaseModel):
    case_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    content_id: str = Field(min_length=1)
    risk_signals: list[str] = Field(default_factory=list)
    evidence: list[EvidenceItem] = Field(default_factory=list)


class ContentSummaryResponse(BaseModel):
    case_id: str
    decision_chain: str
    evidence_count: int
    summary: str


app = FastAPI(title="Content Quality Copilot", version="0.1.0")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"service": "content-quality-copilot", "status": "ok"}


@app.post("/copilot/content/summary", response_model=ContentSummaryResponse)
def summarize_content_case(payload: ContentSummaryRequest) -> ContentSummaryResponse:
    signal_phrase = ", ".join(payload.risk_signals) if payload.risk_signals else "no explicit signals"
    summary = (
        f"Case {payload.case_id} for content {payload.content_id} is queued for operator review. "
        f"Title '{payload.title}' carries signals: {signal_phrase}. "
        "This response is AI-assisted guidance; final action follows rules and human review."
    )
    return ContentSummaryResponse(
        case_id=payload.case_id,
        decision_chain="rules-first-ai-enhancement",
        evidence_count=len(payload.evidence),
        summary=summary,
    )

