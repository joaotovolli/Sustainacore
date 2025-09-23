from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

app = FastAPI()

class Answer(BaseModel):
    answer: str
    sources: list[str] = Field(default_factory=list)
    meta: dict = Field(default_factory=dict)


FALLBACK_MESSAGE = (
    "I couldn’t find a direct answer in the indexed docs. Here are the most relevant sources."
)

@app.get("/ask2", response_model=Answer)
def ask2(q: str = Query(""), k: int = Query(4)):
    # Minimal contract so APEX never breaks.
    if not q.strip():
        return Answer(answer="Ask me something about ESG/AI.", sources=[], meta={"k": k, "note": "empty query"})
    # Replace with your real retrieval/generation logic.
    computed_answer = f"Echo: {q}"
    sources: list[str] = []  # Populate with real source titles/urls when available.

    if not computed_answer.strip():
        limited_sources = sources[:3]
        return Answer(
            answer=FALLBACK_MESSAGE,
            sources=limited_sources,
            meta={"k": k, "note": "fallback", "original_answer": computed_answer},
        )

    return Answer(answer=computed_answer.strip(), sources=sources, meta={"k": k, "note": "stub"})
