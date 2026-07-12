# TECH100 corporate-action and rebalance forensics

Generated 2026-07-12 using read-only Oracle queries through the VM1 service environment.

## Confirmed event

CRWD had a four-for-one forward split, with split-adjusted trading beginning 2026-07-02. The issuer's
Form 10-Q states the ratio, distribution timing, and trading date:
https://www.sec.gov/Archives/edgar/data/1535527/000153552726000025/crwd-20260430.htm

Oracle independently corroborates the event. Canonical adjusted close changed from 772.73999 on
2026-07-01 to 193.98 on 2026-07-02, while 0.07271791187115895 synthetic shares remained unchanged.
The stored constituent return was -0.7489711901670832 and contribution was -0.03049258661545873.
TECH100 consequently recorded -0.03588340969877046 for the day.

The pre-event adjusted series was not retroactively rebased. The stored adjusted prices therefore
mixed two bases and did not preserve the documented adjusted-closing-price methodology.

## Rebalance reconstruction

Stored shares were reverse-engineered into their implied anchor prices and compared with exact
canonical adjusted prices on the immediately previous trading day.

| Rebalance | Previous trading day | Entrants | Missing exact anchors | Stored-share anchor failures |
| --- | --- | ---: | ---: | ---: |
| 2025-11-04 | 2025-11-03 | 0 | 0 | 25 |
| 2025-11-21 | 2025-11-20 | 0 | 0 | 25 |
| 2026-01-02 | 2025-12-31 | 3 | 2 | 1 |
| 2026-04-01 | 2026-03-31 | 4 | 4 | 21 |
| 2026-07-01 | 2026-06-30 | 5 | 0 | 25 |

The initial 2025-01-02 basket has no preceding index trading day and is not classified as an anchor
failure. Quarterly rebalances through 2025-10-01 reconcile within numerical tolerance.

Earliest unreliable published date: **2025-11-04**. Because subsequent holdings and divisors depend
on earlier levels, and because adjusted-history refresh can change earlier anchors, the safe repair
window starts at the 2025-01-02 base date.

## Other anomaly candidates

Fifteen active-constituent adjusted-price moves exceeded 20% in absolute value. CRWD is confirmed by
regulatory evidence. The other fourteen remain unresolved candidates; none is automatically treated
as a split. Publication must fail closed when a candidate is unresolved.

Contribution sums differ from the stored index return by more than 1e-6 on four dates. Stored market
value equals shares multiplied by price within 1e-6 for every constituent row.

## Affected objects

`SC_IDX_PRICES_CANON`, `SC_IDX_HOLDINGS`, `SC_IDX_DIVISOR`, `SC_IDX_LEVELS`,
`SC_IDX_CONSTITUENT_DAILY`, `SC_IDX_CONTRIBUTION_DAILY`, `SC_IDX_STATS_DAILY`,
`SC_IDX_PORTFOLIO_ANALYTICS_DAILY`, `SC_IDX_PORTFOLIO_POSITION_DAILY`, and
`SC_IDX_PORTFOLIO_OPT_INPUTS`.

## Methodology decision

Preserve consistently adjusted closing prices. Refresh the affected ticker history onto one adjusted
basis, recompute every dependent rebalance, and rebuild all derived history from the base date. Do not
also multiply synthetic shares for the same action, because that would double-adjust the split.

## Rollback scope

Before applying, create timestamped backups of the impacted canonical prices, corporate-action rows,
holdings, divisors, levels, constituent history, contributions, statistics, portfolio analytics,
portfolio positions, and optimizer inputs. Restore all objects from the same backup tag as one
controlled rollback.
