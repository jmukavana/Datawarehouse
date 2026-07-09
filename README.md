# Bancassurance Data Warehouse

A local-first dimensional data warehouse for a simulated Kenyan bancassurance
business — policies, premium collections (M-Pesa, bank, cheque, cash), and
claims — built to production patterns and designed to port to BigQuery by
swapping a dbt adapter.

**Stack:** DuckDB · dbt · dlt · Dagster · Python

```
generator (Python, seeded)              dbt (dev = DuckDB, prod = BigQuery)
        │                                        │
        ▼                                        ▼
data/raw/<source>/<load_date>/*.parquet ──dlt──▶ raw_* schemas ──▶ staging ──▶ intermediate ──▶ marts
        (the "data lake";                 (warehouse.duckdb)       stg_*         int_*          fct_* / dim_*
         maps 1:1 onto GCS later)                                                            + SCD2 snapshots
```

Orchestrated by Dagster: every dbt model is an asset in the lineage graph, and
`dbt build` runs tests inline so a failing test fails the run.

## The dimensional model

Kimball star schema, designed before any code in [`docs/design.md`](docs/design.md)
(grain statements, bus matrix, SCD decisions):

- **`fct_premium_payments`** — grain: one row per premium payment received. Incremental.
- **`fct_claim_events`** — grain: one row per claim *status transition* (insert-only event stream). Incremental.
- **`dim_customer`** — **SCD Type 2** on risk tier (dbt snapshot): facts join the
  customer version valid on the event date, not today's.
- **`dim_agent`** — **SCD Type 2** on branch assignment: production credits the
  branch the agent belonged to at the time of sale.
- `dim_policy`, `dim_product`, `dim_branch`, `dim_date` (Kenyan fiscal year Jul–Jun),
  `dim_payment_channel` (seed).

## Deliberately messy sources — and where each mess is fixed

The generator injects the problems warehouses exist to solve:

| Injected problem | Source | Fixed in |
|---|---|---|
| Duplicate transaction pushes (same receipt, sometimes next day's file) | `mpesa_gateway` | `int_payments_deduplicated` |
| Naive Africa/Nairobi timestamps without offset | `mpesa_gateway` | `stg_mpesa_gateway__payments` (-3h to true UTC) |
| Schema drift: `paybill_ref` column appears mid-history | `mpesa_gateway` | dlt schema evolution + column guard in staging |
| Amounts as strings with thousands separators (`"12,500.00"`) | `bank_payments` | `stg_bank_payments__payments` |
| Mixed D/M/Y and ISO value dates in one column | `bank_payments` | `stg_bank_payments__payments` (dual-format parse) |
| Claim events arriving up to 14 days late | `core_insurance` | incremental facts with 14-day lookback window |
| In-place customer updates (risk re-rating) | `core_insurance` | `customers_snapshot` → SCD2 `dim_customer` |
| Occasional NULL national IDs | `core_insurance` | typed/nullable staging + tests |

Verified on a ~95-day simulation: 630 raw M-Pesa rows → 613 fact rows
(17 duplicates collapsed, exactly the injected count); 215 claim events arrived
late and still landed via the lookback; 58 payments resolve to *historical*
customer versions through the point-in-time SCD2 join.

## Sample answers the warehouse gives

**Loss ratio by product line** (`analyses/loss_ratio_by_product.sql`)

| line_of_business | premiums_collected | claims_paid | loss_ratio |
|---|---:|---:|---:|
| HEALTH | 3,850,878 | 4,379,131 | 1.137 |
| PROPERTY | 458,167 | 495,765 | 1.082 |
| LIFE | 2,227,789 | 2,254,659 | 1.012 |
| PERSONAL_ACCIDENT | 146,886 | 89,755 | 0.611 |
| MOTOR | 1,816,323 | 891,519 | 0.491 |
| TRAVEL | 182,522 | 55,355 | 0.303 |

**Premium collection lag by channel** (`analyses/collection_lag_by_channel.sql`)

| channel | payments | avg lag (days) | % within 3 days |
|---|---:|---:|---:|
| M-Pesa Paybill | 623 | 1.8 | 93.3 |
| Cash at Branch | 34 | 2.6 | 73.5 |
| Bank Transfer | 372 | 4.1 | 48.9 |
| Cheque | 97 | 6.0 | 17.5 |

**Agent attribution across branch transfers** (`analyses/agent_branch_production.sql`) —
premium credited to the branch the agent belonged to *when each payment landed*,
which only works because `dim_agent` is Type 2.

## Data quality

- 60 dbt data tests: `unique`/`not_null` on every surrogate key, `relationships`
  on every fact FK, `accepted_values` on statuses.
- Custom tests: claim incidents can't predate policy start; payments must be
  positive; SCD2 validity windows must never overlap.
- Every run writes node-level results (status, rows, duration) to `ops.run_log` —
  the warehouse monitors itself.

## Running it

```bash
uv sync

# One shot, end to end (generate next day → ingest → dbt build):
uv run dagster asset materialize --select '*' -m orchestration.definitions

# Or the pieces:
uv run python -m generator --start-date 2026-01-05 --days 30   # resumes from saved state
uv run python -m ingestion
cd dbt_project && uv run dbt deps && uv run dbt build

# Dagster UI (asset graph, daily schedule):
uv run dagster dev

# Lineage documentation:
cd dbt_project && uv run dbt docs generate && uv run dbt docs serve
```

## BigQuery migration path (roadmap)

The dbt project already carries two targets (`profiles.yml`): `dev` = DuckDB,
`prod` = BigQuery. The migration is: land the same Parquet partitions in GCS,
point dlt at the BigQuery destination, configure the `prod` profile, and
`dbt build --target prod`. Model SQL stays put — cross-database macros
(`dbt_utils`, `extract()`-based date keys) were used throughout. CI plan:
GitHub Actions runs `dbt build` against DuckDB on PRs (free, fast), deploys to
BigQuery on merge.

## Repo layout

```
docs/design.md      dimensional design: bus matrix, grains, SCD decisions
generator/          seeded synthetic-data engine (resumable daily batches)
ingestion/          dlt pipelines: Parquet partitions → DuckDB raw schemas
dbt_project/        staging → intermediate → marts, snapshots, tests, analyses
orchestration/      Dagster assets, daily schedule, dbt integration
data/raw/           the local "data lake" (gitignored)
```
