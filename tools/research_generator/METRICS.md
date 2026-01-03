# Research Generator Metrics

## Core vs Coverage
- Core Index (Top 25): rows with `port_weight > 0`
- Coverage Universe (All 100): all rows for the latest `port_date`

## Sector Exposure
- Core weighted exposure: normalize weights to sum 1, then sum by sector
- Coverage count exposure: sector_count / N * 100
- Core count exposure (equal-weight view): sector_count / N_core * 100
- Delta: new - previous for each exposure
- Sanity check: core count delta must be close to a multiple of (100 / N_core); otherwise flag as inconsistent

## AIGES Composite
- Mean (core weighted): weighted mean using normalized core weights
- Mean (coverage unweighted): arithmetic mean
- Median and IQR: computed on unweighted values

## Concentration (Core)
- Top5 weight share: sum of top 5 core weights / total core weight
- HHI: sum of squared normalized weights

## Breadth (Core)
- Breadth %: % of core names with positive delta in AIGES vs previous

## Turnover (Core)
- Turnover = 0.5 * sum(|w_new - w_old|) using normalized core weights

## Movers (Core)
- Biggest weight movers: top absolute weight delta vs previous
- Biggest score movers: top absolute AIGES delta vs previous

## Notes
- No stock prices are used; index levels are aggregate-only.
- Narrative must remain research/education and avoid investment advice.
