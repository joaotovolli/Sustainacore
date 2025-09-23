from fastapi import FastAPI, Query
from pydantic import BaseModel

app = FastAPI()

class Answer(BaseModel):
    answer: str
    sources: list[str] = []
    meta: dict = {}

@app.get("/ask2", response_model=Answer)
def ask2(q: str = Query(""), k: int = Query(4)):
    # Minimal contract so APEX never breaks.
    if not q.strip():
        return Answer(answer="Ask me something about ESG/AI.", sources=[], meta={"k": k, "note": "empty query"})
    # Replace with your real retrieval/generation logic.
    return Answer(answer=f"Echo: {q}", sources=[], meta={"k": k, "note": "stub"})
