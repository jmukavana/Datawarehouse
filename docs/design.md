# Bancassurance Data Warehouse — Dimensional Design

**Status:** Draft v1 · **Author:** Jamlech Mukavana · **Date:** 2026-07-03

## 1. Business context

A Kenyan bancassurance operation sells insurance products (motor, life, health,
personal accident, last-expense) through bank branches and tied agents. Customers
pay premiums via M-Pesa, bank transfer, cheque, or cash at a branch. Claims are
lodged against active policies and move through a status workflow
(`REGISTERED → ASSESSED → APPROVED/REJECTED → PAID/CLOSED`).

The warehouse answers questions the business actually asks:

- **Loss ratio** by product, branch, and underwriting year (claims paid ÷ premiums earned).
- **Premium collection lag** by payment channel — how many days after due date does money arrive, and is M-Pesa faster than bank transfer?
- **Agent performance** — new business, persistency/retention cohorts, and how performance shifts when an agent moves branches.
- **Customer risk migration** — how customers move between risk tiers over time (requires point-in-time history, hence SCD2).

## 2. Business processes and bus matrix

Each row is a business process that becomes one fact table. Columns are the
conformed dimensions shared across processes.

| Business process (fact) | Date | Customer | Policy | Product | Agent | Branch | Payment Channel |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| Premium payment collection (`fct_premium_payments`) | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |
| Claim lifecycle events (`fct_claim_events`) | ✔ | ✔ | ✔ | ✔ | | ✔ | |
| Policy transactions — issue/endorse/cancel/lapse (`fct_policy_transactions`) | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | |

`fct_policy_transactions` is a stretch goal; the MVP ships the first two facts.
All three share the same conformed dimensions, which is the point of the bus matrix:
loss ratio joins premiums and claims through `dim_product`, `dim_branch`, and `dim_date`
without any remapping.

## 3. Fact tables

### 3.1 `fct_premium_payments`

- **Grain:** one row per premium payment received (a receipt). A policy with monthly
  premiums produces ~12 rows/year. Partial payments and duplicate receipts are
  separate rows in the source; deduplication happens in the intermediate layer, and
  the fact carries only surviving rows.
- **Type:** transaction fact, **incremental** materialization (new rows appended by
  `payment_loaded_at` watermark; late-arriving rows handled via a lookback window).

| Column | Type | Notes |
|---|---|---|
| `payment_key` | surrogate | `generate_surrogate_key(receipt_number, source_system)` |
| `date_key` | FK → `dim_date` | payment received date |
| `due_date_key` | FK → `dim_date` | role-playing date dimension |
| `customer_key` | FK → `dim_customer` | **point-in-time SCD2 key** — the customer version valid at payment date |
| `policy_key` | FK → `dim_policy` | |
| `product_key` | FK → `dim_product` | |
| `agent_key` | FK → `dim_agent` | collecting/servicing agent |
| `branch_key` | FK → `dim_branch` | |
| `payment_channel_key` | FK → `dim_payment_channel` | |
| `receipt_number` | degenerate dim | M-Pesa transaction code (e.g. `QGH7XK4M2P`) or bank ref |
| `premium_due_amount` | measure | fully additive |
| `amount_paid` | measure | fully additive |
| `collection_lag_days` | measure | `payment_date - due_date`; additive only for averaging — document as semi-additive in dbt |

### 3.2 `fct_claim_events`

- **Grain:** one row per claim **status transition** (not one row per claim). A claim
  that goes `REGISTERED → ASSESSED → APPROVED → PAID` produces 4 rows. This grain keeps
  the fact insert-only (no updates), which is what makes incremental loading trivial;
  "current claim status" is a simple `qualify row_number()` view on top.
- **Type:** transaction fact, incremental.

| Column | Type | Notes |
|---|---|---|
| `claim_event_key` | surrogate | `generate_surrogate_key(claim_number, event_sequence)` |
| `date_key` | FK → `dim_date` | event date |
| `customer_key` | FK → `dim_customer` | point-in-time SCD2 key |
| `policy_key` | FK → `dim_policy` | |
| `product_key` | FK → `dim_product` | |
| `branch_key` | FK → `dim_branch` | handling branch |
| `claim_number` | degenerate dim | |
| `event_status` | attribute | REGISTERED / ASSESSED / APPROVED / REJECTED / PAID / CLOSED |
| `claimed_amount` | measure | populated at REGISTERED |
| `assessed_amount` | measure | populated at ASSESSED |
| `paid_amount` | measure | populated at PAID; fully additive |

## 4. Dimensions

| Dimension | SCD type | Natural key | Notable attributes |
|---|---|---|---|
| `dim_date` | static | `date_day` | fiscal year (Kenyan FY: Jul–Jun), month, is_weekend, is_holiday |
| `dim_customer` | **Type 2** | `national_id` / `customer_number` | name, county, segment (retail/SME), **risk_tier** (drives SCD2), kyc_status |
| `dim_policy` | Type 1 | `policy_number` | status, start/end date, sum insured band, payment frequency |
| `dim_product` | Type 1 | `product_code` | line of business, underwriter, commission rate |
| `dim_agent` | **Type 2** | `agent_code` | name, license number, **branch assignment** (drives SCD2), hire date |
| `dim_branch` | Type 1 | `branch_code` | name, county, region, tier |
| `dim_payment_channel` | Type 1 (seed) | `channel_code` | MPESA / BANK_TRANSFER / CHEQUE / CASH; small static table loaded as a dbt seed |

### SCD2 decisions (the interview-question section)

**`dim_customer` — risk tier history.** When underwriting re-rates a customer from
`STANDARD` to `HIGH_RISK`, we must be able to answer "what was their tier *when this
premium was paid*", not just their current tier. Implemented as a **dbt snapshot**
(`timestamp` strategy on `updated_at`) producing `dbt_valid_from` / `dbt_valid_to`.
Facts join on natural key **and** event date between the validity window, so each fact
row carries the point-in-time `customer_key`.

**`dim_agent` — branch assignment history.** Agents transfer between branches; a
branch manager's numbers must credit production to the branch the agent belonged to
*at the time of sale*. Same snapshot mechanism.

Every SCD2 dimension also carries an `is_current` flag and an "current-state" view
(`dim_customer_current`) so analysts who don't need history aren't forced to write
window predicates.

### Surrogate key strategy

`dbt_utils.generate_surrogate_key()` (MD5 hash of natural key + validity start for
SCD2 rows) rather than sequences — deterministic, idempotent across full refreshes,
and portable between DuckDB and BigQuery, which don't share a sequence mechanism.

## 5. Source systems and deliberate mess

Three simulated sources land as Parquet in `data/raw/<source>/<load_date>/`:

| Source | Contents | Injected problems |
|---|---|---|
| `core_insurance` | customers, policies, endorsements, claims | late-arriving claim events (up to 14 days), customer records updated in place (feeds the snapshot), occasional NULL national_id |
| `mpesa_gateway` | M-Pesa premium payments | duplicate transaction pushes (same code, two files), timestamps in `Africa/Nairobi` local time **without** offset, schema drift (a `paybill_ref` column appears mid-history) |
| `bank_payments` | bank/cheque premium payments | amounts as strings with thousands separators, D/M/Y vs ISO date format drift |

Each mess maps to a named fix in the dbt intermediate layer (e.g.
`int_payments_deduplicated`, `int_payments_unioned`) so the README can point at
problem → solution pairs.

## 6. Architecture and portability

```
generator (Python, Faker)          dbt-duckdb  →  dbt-bigquery (swap adapter)
        │                                │
        ▼                                ▼
data/raw/*.parquet ──dlt──▶ raw schema ──▶ staging (views) ──▶ intermediate ──▶ marts (star)
   (= GCS later)          (DuckDB file)     stg_*                int_*           fct_* / dim_*
                                                                              + snapshots (SCD2)
```

- **Naming:** `stg_<source>__<entity>`, `int_<entity>_<verb>`, `fct_*` / `dim_*`.
- **dbt targets:** `dev` = DuckDB (local file), `prod` = BigQuery. No engine-specific
  SQL outside `dbt` cross-database macros; where unavoidable, `dispatch` macros.
- **Tests:** `unique` + `not_null` on every surrogate key, `relationships` on every FK,
  plus custom tests — claims cannot predate policy start; `amount_paid > 0`;
  SCD2 validity windows must not overlap per natural key.
- **Ops schema:** each run logs rows loaded, test pass/fail counts, and duration into
  `ops.run_log` — the warehouse monitors itself.

## 7. Open questions

- [ ] Orchestrator: Dagster (asset-based, native dbt integration, new keyword) vs
      Airflow (already known, reinforces existing skill). Leaning Dagster.
- [ ] `fct_policy_transactions` in MVP or fast-follow?
- [ ] Premiums *earned* vs *written* for loss ratio — MVP uses written (simpler);
      earned requires a periodic-snapshot fact (`fct_policy_month`), a good v2.
