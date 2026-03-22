# TECH100 Portfolio Analytics
## Methodology, Data Contract and Limitations

Version 1.0  
Document date: 20 March 2026  
Scope baseline: live `https://sustainacore.org/tech100/portfolio/` page and the currently merged backend implementation

This paper documents the methodologies currently implemented on the live TECH100 Portfolio Analytics page. It is intentionally narrower than a forward-looking research roadmap. Every material statement in this document is grounded in the live page, merged implementation, merged PR history, or the published TECH100 methodology page. Where the product deliberately does not support an analytic family, this paper states that explicitly.

The page is a daily portfolio-research workspace built on top of the live TECH100 index stack. It compares the official TECH100 benchmark portfolio with five supported model variants, all derived from the same live benchmark universe and the same merged Oracle-backed analytics pipeline.

Research only. Not investment advice.

## Executive Summary

The live TECH100 Portfolio Analytics page currently supports six daily portfolio series:

| Code | Live page label | Construction summary |
| --- | --- | --- |
| `TECH100` | Official TECH100 | The official live benchmark portfolio and level series as published by the TECH100 index stack |
| `TECH100_EQ` | Equal Weight | The current TECH100 names reweighted equally at rebalance dates |
| `TECH100_GOV` | Governance Tilt | Benchmark weights tilted by the live governance composite already present in the TECH100 dataset |
| `TECH100_MOM` | Momentum Tilt | Benchmark weights tilted by a supported 20-trading-day momentum signal |
| `TECH100_LOWVOL` | Low Volatility Tilt | Benchmark weights tilted toward a supported 60-trading-day low-volatility signal |
| `TECH100_GOV_MOM` | Governance + Momentum | Benchmark weights tilted by the combined governance and momentum rank multipliers |

The methodology has four defining characteristics.

First, the page is benchmark-first. All supported model portfolios are derived from the official live TECH100 benchmark membership and rebalance calendar. The page does not introduce a separate investable universe, a separate security-selection engine, or a separate factor library.

Second, the implementation is deliberately constrained. Governance, momentum, low volatility, concentration, drawdown, sector tilt, attribution windows, and precomputed optimizer-ready inputs are supported because they are demonstrably implemented. Classical value, fundamental quality, dividend yield, small-cap, fundamentally weighted portfolios, a live optimizer, and a full institutional factor risk model are intentionally deferred.

Third, the analytics are daily and precomputed. The page is not an intraday trading surface. Levels, returns, concentrations, exposures, holdings views, and attribution windows are computed in the Oracle-backed daily update chain and then rendered in the page workspace.

Fourth, freshness is operationally integrated. The normal SC_IDX update sequence computes index data first and then refreshes portfolio analytics immediately afterward. In the normal successful state, the portfolio analytics effective date matches the official TECH100 index trade date. If a refresh is genuinely pending, the page keeps a single effective date and may show a compact warning rather than permanent dual-date clutter.

## 1. Product Scope and Reader Guide

The live page is organized as a research workspace rather than a narrow performance widget. Its methodology therefore covers both the analytics engine and the logic that determines what is shown to the reader.

The live page currently contains five primary surfaces:

1. `Daily model tape`: the latest close across the supported model set, including YTD return, 20-day volatility, drawdown, Top 5 concentration, and governance composite.
2. `Analytics workspace`: a selected-model surface with benchmark selection, chart controls, summary cards, and selected-model interpretation.
3. `Comparison table and factor lens`: a same-date cross-model comparison of the supported daily analytics.
4. `Holdings, attribution, and sector tilt`: a composition surface showing top holdings, top and bottom contributors, and sector active weights when sector rows are present.
5. `Method, inputs, and limits`: research inputs, constraint rows, supported analytics, and intentionally deferred analytics.

The page is intentionally narrow in two ways.

It covers only the currently supported model families and only the currently supported daily analytics. It does not expose unsupported factor families merely because they are common in broader asset-management literature.

It also distinguishes between two layers of comparison:

- The storage layer defines active weights, sector tilts, and factor exposures relative to the official TECH100 benchmark.
- The page interaction layer allows the user to compare the selected model against any supported model for chart spreads and summary deltas.

That distinction matters and is documented in Section 8.

## 2. Data Sources and Implementation Scope

### 2.1 Source hierarchy

The portfolio analytics refresh uses only existing TECH100 and SC_IDX objects. The live implementation reads from:

| Source object | Role in methodology |
| --- | --- |
| `SC_IDX_LEVELS` | Official TECH100 index level series and official latest trade date for parity checks |
| `SC_IDX_STATS_DAILY` | Official daily benchmark statistics when available, including selected return, volatility, drawdown, concentration, and imputation fields |
| `SC_IDX_CONSTITUENT_DAILY` | Official daily constituent weights, rebalance dates, and price-quality flags |
| `SC_IDX_CONTRIBUTION_DAILY` | Official daily constituent returns and one-day contributions |
| `TECH11_AI_GOV_ETH_INDEX` | Governance metadata, sector classification, company name, AIGES composite, and the five governance pillar scores |
| `SC_IDX_PRICES_CANON` | Daily price history used to build non-official model paths, using `canon_adj_close_px` when available and otherwise `canon_close_px` |

The refresh writes additive objects only:

| Output object | Role on the live page |
| --- | --- |
| `SC_IDX_PORTFOLIO_ANALYTICS_DAILY` | Daily summary analytics by model and trade date |
| `SC_IDX_PORTFOLIO_POSITION_DAILY` | Daily position-level rows by model and trade date |
| `SC_IDX_PORTFOLIO_OPT_INPUTS` | Precomputed optimizer-ready research inputs on rebalance dates |
| `SC_IDX_MODEL_PORTFOLIO_CONSTRAINTS` | Model-level rule and constraint descriptors |
| `SC_IDX_PORTFOLIO_SECTOR_DAILY_V` | Sector-level aggregation view derived from position rows |

### 2.2 Source interpretation

The official benchmark on this page is not reconstructed from scratch. It is taken from the live official TECH100 daily objects. The model variants are the synthetic layer added on top of those official daily objects.

The governance composite used on this page is the `AIGES_COMPOSITE_AVERAGE` field in `TECH11_AI_GOV_ETH_INDEX`. The published TECH100 methodology page defines that composite as:

```text
AIGES Composite Average
= (Transparency + Ethical Principles + Governance Structure
   + Regulatory Alignment + Stakeholder Engagement) / 5
```

The portfolio page consumes that existing governance composite. It does not define a separate governance score.

### 2.3 Units and display conventions

The storage layer generally uses decimal units:

- weights are decimals, for example `0.0425 = 4.25%`
- returns are decimals, for example `0.018 = 1.8%`
- volatility is a decimal annualized rate
- contribution is a decimal portfolio-return contribution

The live page renders:

- weights and returns as percentages
- contribution windows in basis points
- governance composite as points on the underlying 0-100 AIGES scale
- index level as an index-point series rather than a currency NAV

## 3. Benchmark Definition

### 3.1 Official benchmark as represented on this page

The live portfolio page defines the official benchmark from the live TECH100 index objects:

- daily level series: `SC_IDX_LEVELS`
- daily benchmark statistics: `SC_IDX_STATS_DAILY`
- daily constituent weights and rebalance dates: `SC_IDX_CONSTITUENT_DAILY`
- daily constituent returns and one-day contributions: `SC_IDX_CONTRIBUTION_DAILY`

In other words, the official TECH100 row on the page is the live official benchmark portfolio, not a shadow approximation.

### 3.2 Relationship to the published TECH100 index methodology

The separate TECH100 methodology page states that the index methodology ranks companies by the AIGES composite and assigns the top 25 equal weights at each rebalance. The portfolio analytics page does not re-document the full index methodology stack. Instead, it inherits the official benchmark as already published in the live daily TECH100 tables.

For the purposes of this page:

- official benchmark membership and rebalance dates come from the official daily constituent table
- official daily weights are the live published benchmark weights on each trade date
- official level history is the live published TECH100 level series

This paper therefore treats the official benchmark as the published live benchmark input to the portfolio analytics engine.

## 4. Supported Portfolio Families

### 4.1 Common design principles

All five alternative model portfolios share the same design rules:

- universe: the current live TECH100 benchmark names
- reconstitution calendar: the benchmark rebalance schedule
- investment posture: long-only and fully invested
- security-level weighting basis: the current benchmark weights on rebalance dates
- price source: canonical daily prices from `SC_IDX_PRICES_CANON`

The models differ only in the transformation applied to benchmark weights at rebalance dates.

### 4.2 Notation

Let:

- `i` index securities in the current benchmark universe
- `t` index trade dates
- `r` index rebalance dates
- `N_r` be the number of eligible benchmark names at rebalance date `r`
- `w_i^B(r)` be the official benchmark weight of name `i` on rebalance date `r`
- `G_i(r)` be the governance composite available for name `i`
- `M_i(r-1)` be the 20-day momentum signal measured on the prior trading day to `r`
- `V_i(r-1)` be the 60-day low-volatility signal measured on the prior trading day to `r`

The implementation uses the prior trading day for momentum and low-volatility signals when a rebalance occurs. This avoids same-day signal look-ahead from the rebalance date itself.

### 4.3 Rank-to-multiplier mapping

For governance and momentum, higher signal values are preferred. For low volatility, lower realized volatility is preferred.

For a cross-section with `N` names and rank `k_i` where `k_i = 1` is best:

```text
m_i = 0.5 + (1 - (k_i - 1) / (N - 1))
```

This creates a linear multiplier range from `1.5` for the best-ranked name to `0.5` for the lowest-ranked name.

Two implementation details matter:

1. If only one name is available, its multiplier is set to `1.0`.
2. If a signal is missing for a name, the current implementation assigns a neutral multiplier of `1.0` for weight construction and a worst-after-available rank for rank reporting.

### 4.4 Official TECH100

The official portfolio is simply the published benchmark:

```text
w_i^TECH100(r) = w_i^B(r)
```

No transformation is applied.

### 4.5 Equal Weight

The equal-weight model removes benchmark concentration at the rebalance point:

```text
w_i^EQ(r) = 1 / N_r
```

for every benchmark member with positive official weight on the rebalance date.

### 4.6 Governance Tilt

The governance-tilt model scales benchmark weights by the governance rank multiplier and renormalizes:

```text
raw_i^GOV(r) = w_i^B(r) * m_i^GOV(r)

w_i^GOV(r) = raw_i^GOV(r) / Σ_j raw_j^GOV(r)
```

where `m_i^GOV(r)` is the multiplier implied by the AIGES composite ranking.

### 4.7 Momentum Tilt

The momentum-tilt model scales benchmark weights by the 20-day momentum multiplier and renormalizes:

```text
raw_i^MOM(r) = w_i^B(r) * m_i^MOM(r)

w_i^MOM(r) = raw_i^MOM(r) / Σ_j raw_j^MOM(r)
```

### 4.8 Low Volatility Tilt

The low-volatility model scales benchmark weights by the low-volatility multiplier and renormalizes:

```text
raw_i^LV(r) = w_i^B(r) * m_i^LV(r)

w_i^LV(r) = raw_i^LV(r) / Σ_j raw_j^LV(r)
```

Lower realized volatility receives the stronger multiplier because the ranking is reversed for this model family.

### 4.9 Governance + Momentum

The hybrid model uses the product of the governance and momentum multipliers:

```text
raw_i^GM(r) = w_i^B(r) * m_i^GOV(r) * m_i^MOM(r)

w_i^GM(r) = raw_i^GM(r) / Σ_j raw_j^GM(r)
```

This is an important implementation detail. The live hybrid portfolio uses the product of the two separate rank multipliers. It is not built from a separate optimizer and it is not built from the `hybrid_rank` field shown in the optimizer-input object.

## 5. Signal Methodologies

### 5.1 Governance signal

The governance signal is the AIGES composite stored in `TECH11_AI_GOV_ETH_INDEX` and exposed in the backend as `governance_score`.

At the security level:

```text
G_i = AIGES Composite Average_i
```

The page also stores the five underlying governance pillars at the position level:

- Transparency
- Ethical Principles
- Governance Structure
- Regulatory Alignment
- Stakeholder Engagement

However, the live page summarizes governance primarily through the composite score rather than a pillar-by-pillar dashboard.

### 5.2 Momentum signal

The momentum signal is a simple trailing price ratio over 20 trading days:

```text
M_i(t) = P_i(t) / P_i(t - 20) - 1
```

where `P_i(t)` is the canonical price used by the analytics engine for security `i` on date `t`.

The implementation uses adjusted close when available and otherwise close:

```text
P_i(t) = NVL(canon_adj_close_px, canon_close_px)
```

No residualization, market-neutralization, or industry-neutralization is applied. This is a direct supported signal, not a full factor model.

### 5.3 Low-volatility signal

The low-volatility signal is the annualized population standard deviation of the last 60 daily returns:

```text
r_i(s) = P_i(s) / P_i(s - 1) - 1

V_i(t) = pstdev({r_i(t - 59), ..., r_i(t)}) * sqrt(252)
```

where `pstdev` denotes the population standard deviation. Lower values are better.

The signal is only available when a full 60-return window exists with no missing price observations for that name.

## 6. Portfolio Path Construction

### 6.1 Rebalance timing

Model weights are recomputed when either of the following is true:

- the model has not yet been initialized
- the trade date is an official benchmark rebalance date

On each rebalance date, the model uses:

- the official benchmark weights on that rebalance date
- the most recent governance metadata date less than or equal to that trade date
- the prior-trading-day momentum and low-volatility signals

### 6.2 Share-based path construction

Alternative model paths are implemented through shares, not by chaining synthetic returns alone.

At rebalance date `r`, the model determines target weights `w_i^p(r)` for portfolio `p`. It then converts those weights into shares using an anchor level and anchor prices:

```text
shares_i(r) = Level_p(anchor) * w_i^p(r) / P_i(anchor)
```

where:

- `anchor` is the previous trade date if one exists, otherwise the first available trade date
- `Level_p(anchor)` is the model level at the anchor date if already available; otherwise the official benchmark level at the anchor date

Between rebalances, shares are held constant and weights drift with market values:

```text
MV_i(t) = shares_i(r) * P_i(t)

Level_p(t) = Σ_i MV_i(t)

w_i^p(t) = MV_i(t) / Level_p(t)
```

This design gives the model families realistic weight drift between rebalance dates. The page therefore does not display frozen rebalance weights except on the rebalance date itself.

### 6.3 Official benchmark path

The official benchmark path is taken directly from the official daily objects rather than rebuilt from the share path used for alternative models. This is why the official TECH100 row can inherit official benchmark statistics when they exist in `SC_IDX_STATS_DAILY`.

## 7. Return, Risk and Concentration Analytics

### 7.1 Level series

The analytics table stores a daily level field `LEVEL_TR`. For the official benchmark, this is the published official TECH100 level. For alternative models, this is the model market value generated by the share-based path construction described above.

All model levels are anchored to the same official benchmark starting level so they are visually comparable.

### 7.2 One-day return

For non-official models:

```text
Ret_1D,p(t) = Level_p(t) / Level_p(t - 1) - 1
```

For the official benchmark, the system prefers the official one-day return published in the daily benchmark object when available.

### 7.3 Multi-period returns

The 5-day and 20-day returns are simple level ratios:

```text
Ret_L,p(t) = Level_p(t) / Level_p(t - L) - 1
```

for `L = 5` and `L = 20`.

Month-to-date and year-to-date returns are defined relative to the first available trading day in the current month or year:

```text
Ret_MTD,p(t) = Level_p(t) / Level_p(t_month_start) - 1

Ret_YTD,p(t) = Level_p(t) / Level_p(t_year_start) - 1
```

The first trading day of a period does not show a period return because a positive lookback is required in the implementation.

### 7.4 Rolling volatility

The analytics layer computes 20-day and 60-day annualized rolling volatility from daily portfolio returns using population standard deviation:

```text
Vol_L,p(t) = pstdev({Ret_1D,p(t - L + 1), ..., Ret_1D,p(t)}) * sqrt(252)
```

for `L = 20` and `L = 60`.

The window must be complete. If any return in the full required window is missing, the rolling volatility is not populated for that date.

### 7.5 Drawdown

Current drawdown is measured against the running peak:

```text
Peak_p(t) = max(Level_p(s)) for s <= t

Drawdown_p(t) = Level_p(t) / Peak_p(t) - 1
```

This value is stored as `DRAWDOWN_TO_DATE`.

### 7.6 Trailing 252-day maximum drawdown

The 252-day maximum drawdown is computed within a trailing 252-trading-day window:

```text
MaxDD_252,p(t) = min(Drawdown_p(s)) for s in trailing 252-day window ending at t
```

For the official benchmark, the implementation prefers the official benchmark statistic when it is available in `SC_IDX_STATS_DAILY`.

### 7.7 Concentration measures

The live page currently supports four direct concentration measures.

Top 1 weight:

```text
Top1_p(t) = max_i w_i^p(t)
```

Top 5 weight:

```text
Top5_p(t) = sum of the five largest security weights
```

Herfindahl concentration:

```text
H_p(t) = Σ_i (w_i^p(t))^2
```

Factor concentration:

```text
FactorConcentration_p(t) = H_p(t)
```

In the current implementation, `FACTOR_CONCENTRATION` is simply the Herfindahl index stored under a factor-oriented label.

### 7.8 Governance, momentum and low-volatility averages

The page summarizes the average signal profile of each model via weighted averages over the names with non-null signal values:

```text
AvgSignal_p(t) = Σ_{i in A(t)} w_i^p(t) * Signal_i(t) / Σ_{i in A(t)} w_i^p(t)
```

where `A(t)` is the set of names with available values for that signal on date `t`.

This logic is used for:

- `AVG_GOVERNANCE_SCORE`
- `AVG_MOMENTUM_20D`
- `AVG_LOW_VOL_60D`

These are weighted portfolio characteristics, not risk-model betas.

### 7.9 Active factor exposure

The active factor exposure fields are calculated relative to the official benchmark using cross-sectional z-scores on the available cross-section.

For a signal `X_i(t)` with cross-sectional mean `μ_X(t)` and standard deviation `σ_X(t)`:

```text
z_i^X(t) = (X_i(t) - μ_X(t)) / σ_X(t)

Exposure_p^X(t) = Σ_i (w_i^p(t) - w_i^B(t)) * z_i^X(t)
```

This methodology is used for:

- governance exposure
- momentum exposure
- low-volatility exposure

For low-volatility exposure, the implementation first inverts the realized volatility signal:

```text
X_i^LV(t) = 1 / V_i(t)
```

before computing the cross-sectional z-score. This means a model with positive low-volatility exposure is tilted toward lower-volatility names relative to the official benchmark.

If no signal values are available, the exposure is not populated. If the available cross-section has zero dispersion, the exposure is set to `0`.

### 7.10 Sector tilt

The sector-tilt statistic is the half-sum of absolute active sector deviations:

```text
SectorTiltAbs_p(t)
= 0.5 * Σ_s |W_p,s(t) - W_B,s(t)|
```

where:

- `W_p,s(t)` is the model sector weight
- `W_B,s(t)` is the official benchmark sector weight

This is a standard active-share style sector dispersion measure. A value of `0` means the model has the same sector mix as the official benchmark. Larger values indicate greater sector divergence.

## 8. Holdings, Attribution and Sector Methodology

### 8.1 Position-level records

The position table stores one row per model, trade date, and ticker. The live page currently shows the top 12 positions for the selected model, ranked by current model weight. The backend stores the full position set.

Each position row includes:

- current model weight
- official benchmark weight
- active weight relative to the official benchmark
- one-day security return
- contribution windows for `1D`, `5D`, `20D`, `MTD`, and `YTD`
- governance composite and governance pillar scores
- momentum and low-volatility signals
- price-quality flag

### 8.2 Active weight convention

At the storage layer, active weight is always defined against the official TECH100 benchmark:

```text
ActiveWeight_i,p(t) = w_i^p(t) - w_i^B(t)
```

This remains true even if the user selects a different comparison benchmark in the page workspace. The comparator control affects displayed chart spreads and summary deltas, not the stored active-weight definition.

### 8.3 One-day contribution

For alternative portfolios, security-level one-day contribution is arithmetic:

```text
Contrib_1D,i,p(t) = w_i^p(t - 1) * r_i(t)
```

where:

```text
r_i(t) = P_i(t) / P_i(t - 1) - 1
```

For the official benchmark, the implementation prefers the official one-day contribution in `SC_IDX_CONTRIBUTION_DAILY` when it exists.

### 8.4 Contribution windows

The page supports additive attribution windows for `1D`, `5D`, `20D`, `MTD`, and `YTD`.

For a generic window `Ω(t)`:

```text
Contrib_Ω,i,p(t) = Σ_s Contrib_1D,i,p(s) for s in Ω(t)
```

This is an arithmetic cumulative contribution methodology. It is simple, transparent, and consistent with the live implementation, but it is not a full Brinson attribution framework and it is not a geometrically linked attribution model.

Two consequences follow:

1. Window contribution sums are designed for interpretability, not for a complete institutional attribution taxonomy.
2. Summed arithmetic contributions over long windows may not match a geometrically linked total return decomposition exactly.

### 8.5 Sector rows

The sector view is available when sector classifications are present. Position rows are grouped by sector, with missing sectors assigned to `Unclassified`.

For each sector `s`:

```text
SectorWeight_p,s(t) = Σ_{i in s} w_i^p(t)

BenchmarkSectorWeight_s(t) = Σ_{i in s} w_i^B(t)

ActiveSectorWeight_p,s(t) = SectorWeight_p,s(t) - BenchmarkSectorWeight_s(t)
```

Sector contribution windows are simple sums of the constituent contribution rows:

```text
SectorContrib_Ω,p,s(t) = Σ_{i in s} Contrib_Ω,i,p(t)
```

The live sector chart on the page currently emphasizes active sector weight, while the table pairs active sector weight with YTD contribution.

## 9. Research Inputs and Portfolio Rule Exposure

### 9.1 Optimizer-ready input table

The page does not expose a live optimizer, but it does expose a summary of precomputed optimizer-ready inputs.

The underlying input table is created on rebalance dates and contains, for each benchmark member:

- benchmark weight
- governance composite
- 20-day momentum
- 60-day low volatility
- governance rank
- momentum rank
- low-volatility rank
- hybrid rank
- price-quality flag
- eligibility flag

### 9.2 Important interpretation points

The optimizer-input surface should be read as a research-input summary, not as a promise of a live optimization engine.

Three implementation details are especially important:

1. `eligible_flag` is currently a structural inclusion flag for the current benchmark universe, not a separate institutional-grade screening engine.
2. `hybrid_rank` is calculated from a simple sum of governance and momentum values for ranking display purposes.
3. The actual live `TECH100_GOV_MOM` portfolio does not use `hybrid_rank` directly; it uses the product of the separate governance and momentum rank multipliers.

### 9.3 Constraint table

The live page also shows model-level rule descriptors from `SC_IDX_MODEL_PORTFOLIO_CONSTRAINTS`. The currently implemented live constraint keys are:

- `LONG_ONLY = TRUE`
- `FULLY_INVESTED = TRUE`
- `LIVE_OPTIMIZER = FALSE`
- `UNIVERSE_SOURCE = TECH100_TOP25_ACTIVE`
- `REBALANCE_SOURCE = TECH100_REBALANCE_DATES`
- `MAX_CONSTITUENTS = 25`
- `SOLVER_MODE = PRECOMPUTED_ONLY`
- `WEIGHT_CONSTRUCTION = ...` by model family

The weight-construction values currently exposed are:

- `OFFICIAL_BENCHMARK`
- `EQUAL_WEIGHT`
- `BENCHMARK_X_GOVERNANCE_RANK`
- `BENCHMARK_X_MOMENTUM_RANK`
- `BENCHMARK_X_LOW_VOL_RANK`
- `BENCHMARK_X_GOVERNANCE_RANK_X_MOMENTUM_RANK`

This rule exposure is intentionally descriptive. The page does not offer on-demand optimization, scenario solving, transaction-cost minimization, or user-editable constraints.

## 10. Live Page Interpretation

### 10.1 Comparison chart

The workspace chart supports three metric modes:

- `Relative`
- `Volatility`
- `Drawdown`

For relative mode, each model is rebased to the selected range start:

```text
Relative_p(t) = (Level_p(t) / Level_p(t0) - 1) * 100
```

where `t0` is the first date in the selected chart range.

When the user switches the view to `Vs benchmark` and the selected comparator differs from the selected model, the chart shows a spread:

```text
Spread_p,q(t) = Metric_p(t) - Metric_q(t)
```

This applies to relative return, rolling volatility, and drawdown.

### 10.2 Range controls

The live page supports:

- `3M`
- `6M`
- `YTD`
- `1Y`
- `Max`

The backend supplies full history for the supported model set and the range filter is applied in the page layer.

### 10.3 Holdings views

The live page supports three holdings-table views:

- `Core`
- `Signals`
- `Attribution`

These are presentation choices over the same underlying position rows. They do not change the stored methodology.

### 10.4 Benchmark-selection behavior

The `Compare against` control affects:

- chart spreads in active view
- selected summary delta cards
- explanatory text attached to the active comparison

It does not redefine:

- stored active weights
- stored sector active weights
- stored factor exposures

Those stored benchmark-relative analytics remain anchored to the official TECH100 benchmark.

## 11. Daily Update Cycle and Freshness Methodology

### 11.1 Pipeline ordering

The live SC_IDX process integrates portfolio analytics into the normal daily chain. The current orchestrator order is:

```text
update_trading_days
→ ingest_prices
→ completeness_check
→ calc_index
→ portfolio_analytics
→ impute
```

The portfolio refresh is therefore a post-index stage in the same operational sequence, not a separate manual catch-up process.

### 11.2 Operational cadence

The current VM1 scheduler runbook documents four UTC runs per day:

- price ingest at `00:00`, `05:00`, `09:00`, `13:00`
- pipeline at `00:30`, `05:30`, `09:30`, `13:30`

The methodology-relevant point is the sequencing invariant: once official TECH100 index data has been calculated successfully for a trade date, the portfolio analytics refresh is executed immediately afterward in the same chain.

### 11.3 Write-window logic

The portfolio-refresh command:

- determines the available official trade-date bounds from `SC_IDX_LEVELS`
- calculates portfolio outputs across the available range
- writes only the requested or missing window to the portfolio analytics objects
- verifies that the portfolio tables have advanced to the expected official trade date

This makes the portfolio analytics layer part of the normal daily operational flow rather than a separate stale side process.

### 11.4 Effective-date logic on the page

The live page uses:

```text
Portfolio effective date
= MAX(trade_date) from SC_IDX_PORTFOLIO_ANALYTICS_DAILY
```

The operational parity check uses:

```text
Official index date
= MAX(trade_date) from SC_IDX_LEVELS
```

Under normal successful operation, these dates are intended to match.

The live page presentation follows two rules:

1. In the normal state, the page shows one subtle `As of` date only.
2. If the official index has advanced but portfolio analytics have not yet refreshed, the page may show a compact pending-refresh warning while still keeping a single displayed effective date.

The page does not permanently display dual-date messaging as part of normal-state UX.

## 12. Supported Analytics and Intentionally Deferred Analytics

### 12.1 Supported now

The live page and backend currently support:

- Official TECH100 benchmark analytics
- Equal weight, governance tilt, momentum tilt, low-volatility tilt, and governance-plus-momentum model portfolios
- Rolling volatility, drawdown, and concentration analytics
- Governance composite summaries from the existing TECH100 dataset
- Sector weights and sector contribution when sector rows are present
- Attribution windows for `1D`, `5D`, `20D`, `MTD`, and `YTD`
- Precomputed optimizer-ready inputs and portfolio constraints

### 12.2 Explicitly deferred

The live product intentionally does not support the following analytics families at the time of writing:

- Classical value
- Fundamental quality as a separate fundamentals factor family
- Dividend yield
- Small-cap factor families
- Fundamentally weighted portfolios
- A live optimizer or on-demand portfolio solver
- A full institutional factor risk model

These are deferred because the current live product contract does not prove them. This document therefore does not describe them as live features.

## 13. Limitations

The page is a serious daily research surface, but it remains intentionally narrower than an institutional multi-factor portfolio platform. Readers should keep the following limits in view.

### 13.1 No unsupported factor expansion

The model set is intentionally restricted to governance, momentum, low volatility, and their hybrid combination. The live implementation does not claim classical value, fundamental quality, dividend-yield, small-cap, or fundamentally weighted model families.

### 13.2 No live optimizer

The page exposes optimizer-ready research inputs and constraint descriptors, but no live optimization engine is currently deployed. There is no user-triggered solver, no efficient-frontier surface, and no on-page scenario optimizer.

### 13.3 No transaction-cost or turnover model

The current methodology does not apply:

- transaction costs
- market impact
- turnover penalties
- tax assumptions
- capacity constraints

### 13.4 Benchmark-relative storage conventions

Several stored active metrics are always relative to the official TECH100 benchmark even when the page allows the user to compare against a different model in the interaction layer. Users should not interpret the comparator control as redefining the stored active-weight fields.

### 13.5 Attribution simplicity

The contribution windows are additive arithmetic sums of daily contributions. They are useful and transparent, but they are not a full institutional attribution framework such as Brinson-Fachler or geometrically linked multi-period attribution.

### 13.6 Signal availability and metadata timing

Momentum and low-volatility signals require complete trailing windows. Governance metadata uses the most recent available portfolio metadata date up to the current trade date. Missing signal values receive neutral weight treatment rather than aggressive extrapolation.

### 13.7 Price dependence

Alternative model levels depend on the quality and continuity of canonical price history in `SC_IDX_PRICES_CANON`, using adjusted close when available and close otherwise. The methodology does not separately model dividends beyond what is already reflected in the canonical adjusted-price field.

## Appendix A. Notation and Definitions

| Symbol | Definition |
| --- | --- |
| `i` | Security index |
| `t` | Trade date |
| `r` | Rebalance date |
| `p` | Portfolio or model index |
| `s` | Sector index |
| `P_i(t)` | Canonical price for security `i` on date `t` |
| `w_i^B(t)` | Official benchmark weight |
| `w_i^p(t)` | Model portfolio weight |
| `Level_p(t)` | Model level or index-point series |
| `G_i(t)` | Governance composite signal |
| `M_i(t)` | 20-day momentum signal |
| `V_i(t)` | 60-day realized volatility signal |
| `m_i` | Rank-to-multiplier mapping used in tilt construction |

## Appendix B. Field-Level Interpretation

| Live metric | Interpretation |
| --- | --- |
| `LEVEL_TR` | Daily portfolio level in index points |
| `RET_1D`, `RET_5D`, `RET_20D`, `RET_MTD`, `RET_YTD` | Decimal returns over the stated window |
| `VOL_20D`, `VOL_60D` | Annualized rolling population volatility over 20 or 60 daily returns |
| `DRAWDOWN_TO_DATE` | Current drawdown from the running peak |
| `MAX_DRAWDOWN_252D` | Worst drawdown observed in the trailing 252-trading-day window |
| `TOP1_WEIGHT`, `TOP5_WEIGHT` | Concentration of the largest one or five names |
| `HERFINDAHL` | Sum of squared security weights |
| `AVG_GOVERNANCE_SCORE` | Weight-averaged AIGES composite |
| `AVG_MOMENTUM_20D` | Weight-averaged 20-day momentum |
| `AVG_LOW_VOL_60D` | Weight-averaged 60-day realized volatility |
| `FACTOR_GOVERNANCE_EXPOSURE`, `FACTOR_MOMENTUM_EXPOSURE`, `FACTOR_LOW_VOL_EXPOSURE` | Benchmark-relative z-score-weighted exposure measures |
| `FACTOR_SECTOR_TILT_ABS` | Half-sum absolute sector deviation versus the official benchmark |
| `N_IMPUTED` | Number of current constituents flagged as imputed on the trade date |

## Appendix C. What the Live Page Does Not Claim

The page does not claim to provide:

- intraday analytics
- a live optimizer or on-demand solver
- unsupported factor families
- a full institutional factor risk model
- a substitute for the separate official TECH100 methodology paper

It is a daily, benchmark-grounded portfolio analytics workspace built on the live TECH100 stack and limited to the supported methodologies described above.
