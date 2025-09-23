# /opt/sustainacore-ai/wsgi_failover_entry.py
# Optional entrypoint: wrap the app with Ask2LLMOrchestratorMiddleware for empty-answer fill.
from app import app as base_app
try:
    from ask2_llm_orchestrator import Ask2LLMOrchestratorMiddleware
    app = Ask2LLMOrchestratorMiddleware(base_app)
except Exception:
    # If import fails, fall back to raw app so service stays up
    app = base_app
