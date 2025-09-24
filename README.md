# SustainaCore Gateway

This repository exposes the `/ask2` entry-point that APEX uses to power the
SustainaCore assistant. The service now runs a **Gemini-first retrieval and
generation orchestration** while keeping Oracle Autonomous Database + 23ai
vector search as the single system of record.

## Gemini-First Orchestration

### Request Flow

1. **Intent classification** – Every query is sent to Gemini via the CLI to
   decide between `SMALL_TALK` and `INFO_REQUEST`.
2. **Small talk** – Gemini produces a 1–2 sentence reply. No retrieval happens
   and no citations are returned.
3. **Grounded Q&A** – Gemini returns a retrieval plan (filters + 3–5 query
   variants). The local Oracle retriever executes the plan, deduplicates and
   diversifies the top results (≤2 chunks per source, ≤1 per URL, keep the later
   duplicates). Gemini then composes the final answer with inline citations and
   a cleaned `Sources` list.

### Oracle Retriever Contract

* **Inputs** – `filters` (strict metadata), `query_variants` (3–5 strings) and
  `k` (default 24).
* **Scope first** – Metadata filters are applied in SQL before the vector KNN
  call.
* **Vector search** – Executes `VECTOR_DISTANCE` over the `ESG_DOCS`
  embedding column (MiniLM 384) using the cosine metric.
* **Deduplication** – Canonical key is the lowercase of
  `COALESCE(NORMALIZED_URL, SOURCE_ID, DOC_ID)`. Later duplicates override
  earlier ones. Near-duplicates collapse when title + hash(first 200 chars)
  match. Final payload is capped at 6 sources (8 internal facts) with ≤2
  chunks per source and ≤1 per exact URL.
* **Outputs** –

  ```json
  {
    "facts": [
      {
        "citation_id": "FCA_2024_Guidance",
        "title": "…",
        "source_name": "FCA",
        "url": "https://…",
        "date": "2024-11-18",
        "snippet": "…"
      }
    ],
    "context_note": "- filters applied: …\n- knn_top_k=24 → dedup=6 → final=6",
    "latency_ms": 410,
    "candidates": 24,
    "deduped": 8
  }
  ```

### Output Schema

The Gemini composer returns only two customer-facing fields:

* `answer` – short narrative with inline citations (`[citation_id]`).
* `sources` – clean list formatted `Title — Publisher (Date)` (3–6 items,
  deduplicated).

`meta` is preserved for APEX but contains only diagnostics (latency breakdowns,
retriever context notes, debug toggles). Debug artifacts are available under
`meta.debug` only when `SHOW_DEBUG_BLOCK=1`.

### Feature Flags

| Flag | Default | Purpose |
| --- | --- | --- |
| `GEMINI_FIRST_ENABLED` | `1` | Master switch. Flip to `0` to roll back to the pre-Gemini behaviour without redeploying. |
| `SHOW_DEBUG_BLOCK` | `0` | Exposes `meta.debug` for the developer “Deep/Debug” toggle. Keep `0` in production. |
| `ALLOW_HOP2` | `1` | Enables Gemini-requested second-hop retrieval when additional evidence is required. |

Rate limiting is controlled via `ASK2_RATE_WINDOW` (seconds) and
`ASK2_RATE_MAX` (requests per window). Retrieval caps can be tuned with
`RETRIEVER_MAX_FACTS`, `RETRIEVER_FACT_CAP`, and `RETRIEVER_PER_SOURCE_CAP`.

### Runbook

* **Run locally**

  ```bash
  uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080
  ```

* **Acceptance checks** – Validate the membership, definition, and controversy
  scenarios plus small-talk responses. Confirm deduplication by querying with
  URLs that only differ by tracking parameters. Target end-to-end p50 latency is
  ≤4.5s (Oracle ≤1.2s, Gemini compose ≤2.5s, remaining glue ≤0.8s).

* **Observability** – Every `/ask2` call logs intent, filters, K values, final
  source counts, latency breakdowns, and hop counts. A daily “Top-10 Misses”
  plus Gemini/Oracle usage snapshot is emitted at UTC midnight.

* **Rollback** – Set `GEMINI_FIRST_ENABLED=0` and reload the service. The stack
  will short-circuit the Gemini planner/composer while preserving the `/ask2`
  contract.

## Environment

Copy `config/env.sample` and adjust the Oracle wallet location, DB credentials,
Gemini CLI binary, and API key for your environment. All Gemini interactions
use the CLI (`gemini`), so make sure the binary is available on `$PATH` or
override `GEMINI_BIN`.

