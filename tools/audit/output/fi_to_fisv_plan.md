# FI -> FISV migration plan (Phase 0)

Oracle schema/user: WKSP_ESGAPEX

## Connection configuration
- Tools use `db_helper.get_connection()` and `tools/index_engine/env_loader.load_default_env()`.
- Env sources: `/etc/sustainacore/db.env`, `/etc/sustainacore-ai/secrets.env`.
- Expected vars: `DB_USER`, `DB_PASSWORD`, `DB_DSN`, `TNS_ADMIN` (wallet).

## Candidate tables/columns
- AIGES_SCORES: columns=TICKER
  constraints:
  - P:PK_AIGES_SCORES: ASOF, TICKER
- ATTRIBUTION: columns=TICKER
  constraints:
  - P:PK_ATTRIBUTION: DT, TICKER
- ESG_BENCHMARKS: columns=SYMBOL
  constraints:
  - P:SYS_C0029679: BENCH_ID
  - U:SYS_C0029680: CODE
- ESG_COMPANIES: columns=TICKER
  constraints:
  - P:SYS_C0029674: COMPANY_ID
  - U:SYS_C0029675: TICKER
- ESG_CONSTITUENT_PRICES: columns=TICKER
  constraints:
  - P:ESG_CONS_PRICES_PK: INDEX_CODE, TICKER, TRADE_DATE
- INDEX_COMPANIES: columns=TICKER
  constraints:
  - P:SYS_C0029456: TICKER
- INDEX_CONSTITUENTS: columns=TICKER
  constraints:
  - P:PK_CONSTITUENTS: INDEX_DATE, TICKER
- NEWS_ITEMS: columns=TICKER
  constraints:
  - P:SYS_C0029733: ID
- NEWS_ITEM_TICKERS: columns=TICKER
  constraints:
  - P:PK_NEWS_ITEM_TICKERS: ITEM_TABLE, ITEM_ID, TICKER
- SC_IDX_CONSTITUENT_DAILY: columns=TICKER
  constraints:
  - P:SC_IDX_CONSTITUENT_DAILY_PK: TRADE_DATE, TICKER
- SC_IDX_CONTRIBUTION_DAILY: columns=TICKER
  constraints:
  - P:SC_IDX_CONTRIBUTION_DAILY_PK: TRADE_DATE, TICKER
- SC_IDX_HOLDINGS: columns=TICKER
  constraints:
  - P:PK_SC_IDX_HOLDINGS: INDEX_CODE, REBALANCE_DATE, TICKER
- SC_IDX_IMPUTATIONS: columns=TICKER
  constraints:
  - P:SC_IDX_IMPUTATIONS_PK: INDEX_CODE, TRADE_DATE, TICKER
- SC_IDX_PRICES_CANON: columns=TICKER
  constraints:
  - P:PK_SC_IDX_PRICES_CANON: TICKER, TRADE_DATE
- SC_IDX_PRICES_RAW: columns=TICKER
  constraints:
  - P:PK_SC_IDX_PRICES_RAW: TICKER, TRADE_DATE, PROVIDER
- TECH11_AI_GOV_ETH_INDEX: columns=TICKER
  constraints: none found
- V_NEWS_ALL: columns=TICKER
  constraints: none found
- V_NEWS_ENRICHED: columns=TICKER
  constraints: none found
- V_NEWS_RECENT: columns=TICKER
  constraints: none found
- V_NEWS_SEARCH: columns=TICKER
  constraints: none found

## Collision resolution rules (deterministic)
- General: rename FI -> FISV. If both exist for the same key, keep the best row and delete the other.
- SC_IDX_PRICES_RAW (key: TRADE_DATE, PROVIDER): keep row with STATUS='OK'; if tie, prefer non-null ADJ_CLOSE_PX/CLOSE_PX; if tie, latest INGESTED_AT; else keep existing FISV.
- SC_IDX_PRICES_CANON (key: TRADE_DATE): prefer non-null CANON_ADJ_CLOSE_PX/CANON_CLOSE_PX; if tie, higher PROVIDERS_OK; if tie, latest COMPUTED_AT; else keep existing FISV.
- SC_IDX_CONSTITUENT_DAILY (key: TRADE_DATE): prefer non-null PRICE_USED/MARKET_VALUE/WEIGHT; if tie, latest COMPUTED_AT; else keep existing FISV.
- SC_IDX_CONTRIBUTION_DAILY (key: TRADE_DATE): prefer non-null CONTRIBUTION/RET_1D/WEIGHT_PREV; if tie, latest COMPUTED_AT; else keep existing FISV.
- SC_IDX_HOLDINGS (key: INDEX_CODE, REBALANCE_DATE): prefer non-null SHARES/TARGET_WEIGHT; if tie, keep existing FISV.
- SC_IDX_IMPUTATIONS (key: INDEX_CODE, TRADE_DATE): prefer non-null IMPUTED_PRICE; if tie, latest CREATED_AT; else keep existing FISV.
- TECH11_AI_GOV_ETH_INDEX (key: PORT_DATE, TICKER): prefer non-null PORT_WEIGHT; if tie, higher PORT_WEIGHT; else keep existing FISV.
- INDEX_COMPANIES (key: TICKER): prefer non-null NAME/SECTOR/INDUSTRY; else keep existing FISV.
- INDEX_CONSTITUENTS (key: INDEX_DATE): prefer non-null WEIGHT_PERCENT/SHARES_HELD; else keep existing FISV.
- ESG_COMPANIES (key: TICKER): prefer non-null NAME/SECTOR/INDUSTRY; else keep existing FISV.
- ESG_CONSTITUENT_PRICES (key: INDEX_CODE, TRADE_DATE): prefer non-null CLOSE_ADJ/WEIGHT; else keep existing FISV.

## Notes
- Views (V_NEWS_*) are read-only and excluded from writes.
