# NammaMart Quick Commerce — ETL & ELT Pipelines (FDAIE Module 2)

**Author:** Sudarshan · **DAGs:** `nammamart_etl_sudarshan`, `nammamart_elt_sudarshan` · **Warehouse:** `include/warehouse_sudarshan.duckdb`

> **What this is.** Two Apache Airflow pipelines that build NammaMart's morning report from the *same* five CSV exports — one that
> **cleans before loading (ETL, pandas → star schema)** and one that **loads raw first and cleans in SQL (ELT, DuckDB raw → staging → mart)** —
> scheduled for **09:00 IST, Monday–Saturday**, with a data-quality scorecard, a run-audit table, payment reconciliation, PII masking,
> and a recommendation for Sameer (Part G).
>
> **Headline result.** Both pipelines independently produce **the same numbers to the paisa**: 2,308 clean September orders,
> **₹11,34,962.80** delivered net revenue, 142 rejected rows, 181 reconciliation mismatches. After the 2026-10-01 increment both agree on 2,458 orders and ₹12,10,421.30.
> This is enforced by 67 automated tests (`tests/`), not eyeballed.

---

## Contents
0. [Scoreboard, repo map, how to run, what was verified](#0-scoreboard-repo-map-how-to-run-what-was-verified)
1. [Part 0 — Discovery and data profiling](#1-part-0--discovery-and-data-profiling)
2. [Parts A & B — design notes](#2-parts-a--b--design-notes)
3. [Part C — Scheduling](#3-part-c--scheduling)
4. [Part D — Data-quality dimensions](#4-part-d--data-quality-dimensions)
5. [Part E — Data governance](#5-part-e--data-governance)
6. [The six business questions (SQL + results, both pipelines)](#6-the-six-business-questions)
7. [Part F — Observability experiments](#7-part-f--observability-experiments)
8. [Part G — Recommendation to Sameer](#8-part-g--recommendation-to-sameer-cto)
9. [Bonus items, assumptions, limitations](#9-bonus-items-assumptions-limitations)

---

## 0. Scoreboard, repo map, how to run, what was verified

### 0.1 Sameer's ten acceptance criteria

| # | Criterion | How it is met | Proof |
|---|---|---|---|
| 1 | Data ready before 09:00 IST Mon–Sat | Both DAGs: `CronTriggerTimetable("0 9 * * 1-6", timezone="Asia/Kolkata")`, tz-aware `start_date`; a full run takes ≈ 20 s (ETL) / ≈ 40 s (ELT) | §3; `submission/screenshots/` (schedule, next run `03:30 UTC`); the scheduler created `scheduled__2026-10-06T03:30:00+00:00` and it went green |
| 2 | No bad order reaches finance tables | ETL fact holds only rows that passed every Critical check (2,308 of 2,412); ELT marts read only `dq_status <> 'FAIL'` | `tests/nammamart/test_parity.py::test_clean_orders_are_identical`, §5 Q8 |
| 3 | Every raw record kept, untouched | `raw_*` tables: all columns text, every source row (2,412 orders incl. all 104 bad ones), never cleaned or deleted | `test_elt_raw_is_append_only_and_exact_copy`; Exp 2 |
| 4 | Bad rows set aside with a reason | ETL: `include/quarantine/sudarshan/<file>_failed_<run_id>.csv` + `etl_quarantine`; ELT: `dq_status='FAIL'` + `failure_reason` | `submission/evidence/quarantine_sample/`, Q6 |
| 5 | Re-running never double-counts | Fact load = MERGE on `order_id` with row hash; raw load skips an identical file (SHA-256) | Exp 2: 0 rows inserted/updated, counts unchanged; `test_rerun_does_not_change_the_fact_table_or_revenue` |
| 6 | Next-day file loads incrementally | +150 new orders appended, 10 corrections applied as updates, no duplicates | Exp 3; `test_increment_inserts_150_and_updates_10` |
| 7 | Orders reconcile with payments | `etl_payment_reconciliation` / `mart_payment_reconciliation`: one row per mismatch with reason and amount | Q4b; identical in both pipelines |
| 8 | Customer personal data protected | name → initials, email → `pr***@example.com`, phone → `97******21` before any reporting table; raw PII exists only in ELT `raw_customers` (restricted layer) | `test_masking.py`, `test_customer_dimension_and_masking_match`; **scripted proof** `submission/results/pii_masking_check.md` (0 real emails/phones/names outside `raw_customers`, 29 non-raw tables scanned); §5 Q4 |
| 9 | Anyone can see if today's run worked, and why not | Airflow run history + task logs; `pipeline_runs` (one row per run, even a failed one, with the failed task names); `dq_results` | §7 |
| 10 | ETL-or-ELT recommendation | One page for the CTO, based on what the experiments showed | §8 |

### 0.2 Repository map

```
dags/
  nammamart_etl_sudarshan.py        ETL DAG  (extract x5 -> validate -> quarantine -> transform -> load -> reconcile -> report -> audit)
  nammamart_elt_sudarshan.py        ELT DAG  (extract x5 -> load raw -> stage (SQL) -> MERGE increment -> marts (SQL) -> report -> audit)
include/
  data/nammamart/                   the six source CSVs, byte-for-byte as received (never edited)
  nammamart/                        helper package: config, dq_catalog (the rulebook), masking, audit, warehouse, etl/*, elt/*
  sql/                              every ELT transformation is a .sql file here (stg_*.sql, mart_*.sql, raw_load.sql, ...)
  profiling/                        Part 0: profile_data.py, build_issues_log.py
tests/nammamart/                    54 tests: known defects, ETL==ELT parity, idempotency, increment, masking, experiment regressions, SCD2
tests/dags/test_nammamart_dags.py   13 tests: the schedule, timezone, retries, parallel extracts, audit + verdict tasks (Part C, machine-checked)
experiments/                        Part F: make_experiment_data.py, run_experiments.py, results/*.json
submission/                         data_issues_log.csv, business-question SQL + results, evidence, screenshots
SUBMISSION.md                       this document
```
I did not modify `dags/meridian_pipeline.py` or any trainer file. The warehouse is my own file, `include/warehouse_sudarshan.duckdb`.

### 0.3 How to run

```bash
astro dev start                                  # UI at http://localhost:8080 (admin / admin)
astro dev run dags list-import-errors            # expect: no errors
astro dev run dags test nammamart_etl_sudarshan  # one ETL run from the terminal
astro dev run dags test nammamart_elt_sudarshan  # one ELT run
astro dev pytest                                 # the 67 tests (ETL==ELT parity, idempotency, increment, schedule ...)
```
Un-pause both DAGs in the UI to start the 09:00 IST schedule. **Trigger form params** (all optional): `as_of` (treat this IST time as "now"),
`load_increments` (apply `orders_*_increment.csv` found in the data folder; default on), `data_dir`, `warehouse_path` — these make the Part F
experiments non-destructive (they read copies and write copies).
Optional Slack alert: add an Airflow Variable `SLACK_WEBHOOK_URL` (Admin → Variables). Without it the alert is logged instead.

### 0.4 What was verified, and how (please read — this is the honest part)

* **Verified by running:** both DAGs were executed on a **real Apache Airflow 3.3.2** (api-server + scheduler + DAG processor, the same major line as the
  project's Astro Runtime `3.3-2`) running in WSL. They parse with no import errors, ran green on the *scheduler* (a scheduled run, plus the experiment runs),
  and all 10 Part F experiments were executed on both DAGs; the results below are from those runs, not predictions.
* **Not run on this machine:** Docker Desktop / the Astro CLI were not installed here, so I did **not** run `astro dev start`. Differences to expect on Astro:
  Postgres instead of SQLite as the Airflow metadata DB (irrelevant to my code) and DuckDB files on a Docker bind mount (the trainer's `meridian_pipeline` already does this).
  The screenshots in `submission/screenshots/` are of the genuine Airflow 3.3.2 UI running these DAGs; retake them in your Astro UI if the grader needs the Docker look.
* **67 pytest tests pass** (54 run both pipelines on the real data in temp folders and compare them; 13 check the DAG definitions).

### 0.5 Screenshot checklist → files (`submission/screenshots/`)

| Required screenshot | File |
|---|---|
| Both DAGs in Graph view, green run | `04_etl_graph_green_run.png`, `05_elt_graph_green_run.png` |
| Schedule showing 09:00 IST (and next run) | `01_dags_list_schedule_0900_IST.png`, `02_etl_overview_schedule_and_next_run.png` (`0 9 * * 1-6`, Next Run `2026-10-07 09:00:00`, UI on +05:30), `03_elt_overview_…png` |
| One quarantine file | `15_quarantine_file_with_failure_reason.png` (and the real files in `submission/evidence/quarantine_sample/`) |
| Query results: `dq_results`, `pipeline_runs`, payment reconciliation | `14_query_results_pipeline_runs_dq_results_reconciliation.png` (Markdown in `submission/evidence/query_results/`) |
| Parallel extracts / DuckDB lock queue | `06_etl_gantt_parallel_extracts.png`, `07_elt_gantt_parallel_loads_and_marts.png` |
| Failed runs, retries, recovery (Part F) | `08_etl_failed_run_schema_change_exp6.png`, `09_elt_failed_run_schema_change_exp6.png`, `10a_…`, `10b_…`, `11_task_log_schema_change_error.png`, `12_/13_…run_history.png` |
| Bonus trend chart | `16_dq_trend_chart_etl.png` |

All Airflow screenshots are of Airflow 3.3.2 running these two DAGs (see 0.4); the UI timezone is set to IST so times read as the client sees them. They show the local path of the machine they were taken on.

---

## 1. Part 0 — Discovery and data profiling

Run it yourself: `python include/profiling/profile_data.py` (read-only). Everything is read as text first, so nothing is hidden by type conversion.

### 1.1 Profile (all six files)

| File | Rows | Nulls / blanks | Duplicate keys | Ranges that look wrong | Category oddities |
|---|---|---|---|---|---|
| `orders.csv` | 2,412 | `customer_id` 18, `order_ts` 1, `delivery_minutes` 164 (156 are CANCELLED = fine; **8 are DELIVERED = defect**) | `order_id`: 12 keys / 24 rows (identical copies) | `quantity` −3…5; `discount_pct` up to **250**; `order_amount` −1,593…9,576; `delivery_minutes` 8…**1,440**; `order_ts` up to **2027-01-01**, 5 unparseable (`N/A`, `2026-13-05`, `31/09/2026 10:00`) | status: `delivered`×2, `Pending??`×3; payment_mode: `upi`×4, `UPI `×2, `CARD`, `cod`, `Cashh`×2 |
| `payments.csv` | 2,228 | `gateway_ref` 301 (all cash-on-delivery: expected) | `payment_id`: 4 keys / 8 rows | `amount_paid` −1,593…9,576 (10 rows ≤ 0); `payment_ts` up to 2027-01-01 | same payment_mode spellings; status SUCCESS 2,116 / REFUNDED 68 / FAILED 44 |
| `customers.csv` | 405 | `city` 6; email `N/A` ×4 | `customer_id`: 5 keys / 10 rows (same person, **different phone and area**) | `signup_date` up to **2027-01-05** (3 in the future); 7 phones ≠ 10 digits; 3 pincodes `5600XX`; 8 malformed emails | loyalty_tier: `BRONZE`×3, `bronze`×3, `SILVER`×2 |
| `products.csv` | 152 | `category` 3 | `product_id`: 2 keys (`P0030`, `P0140`) — the second row is named "… **(old)**" with a different MRP | `mrp` −203 (P0109); 4 products with `cost_price` > `mrp` (exactly 1.2 × MRP: P0019, P0073, P0074, P0117) | — |
| `stores.csv` | 15 | `zone` 1 (**S07 BTM Layout**) | none | `capacity_orders_per_day` **0** (S12) | zones East 4, West 4, North 3, South 3 |
| `orders_2026-10-01_increment.csv` | 160 | none | none | none | 150 new orders (all 2026-10-01) + **10 corrections** of older orders (DELIVERED → RETURNED) |

### 1.2 Referential integrity (all four relationships)

| Relationship | Result |
|---|---|
| `orders.customer_id → customers` | 18 orders with a **blank** customer; 0 non-blank orphans |
| `orders.product_id → products` | **9** orders point to products `P9xxx` that do not exist |
| `orders.store_id → stores` | **5** orders point to `S17`×3, `S99`, `S00` (test / unknown stores) |
| `payments.order_id → orders` | **8** payments belong to orders `ORD94…99xxxx` that exist nowhere; 0 payments on CANCELLED orders ✅ |
| (extra) DELIVERED orders with no payment row | 27 (28 after standardising `delivered`) |
| (extra) payments with `FAILED` status on DELIVERED orders | 44 (none of them has a later successful retry) |

### 1.3 Business rules from the client

| Rule | Violations |
|---|---|
| `order_amount = qty × unit_price × (1 − disc/100)` | **18** (e.g. `ORD002305` 6,757.5 vs 675.75 — a ×10 slip; `ORD001274` 110 vs 220) |
| Valid `order_status` ∈ {DELIVERED, CANCELLED, RETURNED} | 3 × `Pending??` (+2 casing) |
| Valid `payment_mode` ∈ {UPI, Card, Cash on Delivery, Wallet} | 2 × `Cashh` unfixable; 8 fixable spellings |
| Deliveries 8–35 min; DELIVERED must have a value | **8** DELIVERED with none; **6** with 480/900/1,440 min |
| SUCCESS payment = order amount | **14** (differences of exactly 10/25/50/100 — keying errors) |
| Increment: new orders **and** corrections | 150 + 10 ✅ as described |
| (found) `unit_price` equals the catalogue `mrp` for every order except product P0109 | P0109's MRP is stored as **−203** (sign flipped); orders say 203 |

### 1.4 Data Issues Log

**226 row-level issues across all five data files** (+ the increment file, which is clean but *changes history*). The complete log, one row per (source row, rule) with
the offending raw value (PII masked) is **`submission/data_issues_log.csv`**; it is generated from the very same rulebook the pipelines use
(`python include/profiling/build_issues_log.py`), so the log and the quarantine cannot disagree. Grouped view:

| File | Rule (proposed) | Column | Problem | Dimension | Severity | Rows | Example keys |
|---|---|---|---|---|---|---|---|
| orders | `ORD_CUSTOMER_ID_NOT_NULL` | customer_id | blank | Completeness | Critical | 18 | ORD002255, ORD000701 |
| orders | `ORD_AMOUNT_MATCHES_FORMULA` | qty/price/disc/amount | amount ≠ formula | Accuracy | Critical | 18 | ORD002103, ORD002305 |
| orders | `ORD_ORDER_ID_UNIQUE` | order_id | identical duplicate copy | Uniqueness | Critical | 12 | ORD001462, ORD001441 |
| orders | `ORD_QUANTITY_POSITIVE` | quantity | 0 or negative | Accuracy | Critical | 10 | ORD000469, ORD001296 |
| orders | `ORD_PRODUCT_EXISTS` | product_id | not in products | Consistency | Critical | 9 | ORD000986 |
| orders | `ORD_DELIVERED_HAS_MINUTES` | delivery_minutes | DELIVERED, no minutes | Completeness | Critical | 8 | ORD001554 |
| orders | `ORD_ORDER_TS_NOT_FUTURE` | order_ts | after run time (2026-10-21, 11-02, 2027) | Timeliness | Critical | 7 | ORD002119 |
| orders | `ORD_DELIVERY_MINUTES_MAX` | delivery_minutes | > 120 (480/900/1440) | Accuracy | Critical | 6 | ORD001037 |
| orders | `ORD_ORDER_TS_VALID` | order_ts | unparseable (`N/A`, `2026-13-05`, `31/09/2026`) | Validity | Critical | 5 | ORD000403 |
| orders | `ORD_STORE_EXISTS` | store_id | S17 / S99 / S00 | Consistency | Critical | 5 | ORD001799 |
| orders | `ORD_DISCOUNT_RANGE` | discount_pct | 120 / 150 / 250 | Validity | Critical | 3 | ORD002103 |
| orders | `ORD_STATUS_VALID` | order_status | `Pending??` | Validity | Critical | 3 | ORD000500 |
| orders | `ORD_PAYMENT_MODE_VALID` | payment_mode | `Cashh` | Validity | Critical | 2 | ORD001812 |
| orders | `ORD_ORDER_TS_NOT_NULL` | order_ts | blank | Completeness | Critical | 1 | ORD000362 |
| orders | `ORD_PAYMENT_MODE_STANDARDISED` | payment_mode | `upi`, `UPI `, `CARD`, `cod` → mapped | Validity | Warning | 8 | ORD000159 |
| orders | `ORD_STATUS_CASING` | order_status | `delivered` → upper-cased | Validity | Warning | 2 | ORD001066 |
| payments | `PAY_AMOUNT_POSITIVE` | amount_paid | ≤ 0 | Accuracy | Critical | 10 | PAY0001568 |
| payments | `PAY_ORDER_EXISTS` | order_id | order exists nowhere | Consistency | Critical | 8 | PAY0002219 |
| payments | `PAY_TS_NOT_FUTURE` | payment_ts | future | Timeliness | Critical | 7 | PAY0000396 |
| payments | `PAY_PAYMENT_ID_UNIQUE` | payment_id | duplicate copy | Uniqueness | Critical | 4 | PAY0000509 |
| payments | `PAY_MODE_VALID` | payment_mode | `Cashh` | Validity | Critical | 2 | PAY0000044 |
| payments | `PAY_AMOUNT_MATCHES_ORDER` | amount_paid | SUCCESS ≠ order amount (detail in reconciliation) | Consistency | Warning | 14 | PAY0000354 |
| payments | `PAY_MODE_STANDARDISED` | payment_mode | spelling mapped | Validity | Warning | 8 | PAY0002188 |
| customers | `CUS_CUSTOMER_ID_UNIQUE` | customer_id | same id twice (different phone/area) | Uniqueness | Critical | 5 | C00103, C00252 |
| customers | `CUS_EMAIL_FORMAT` | email | `N/A` or missing `@`/domain | Validity | Warning | 12 | C00337 |
| customers | `CUS_LOYALTY_TIER_CASING` | loyalty_tier | `BRONZE`/`bronze`/`SILVER` | Consistency | Warning | 8 | C00199 |
| customers | `CUS_PHONE_10_DIGITS` | phone | 7–9 digits | Validity | Warning | 7 | C00151 |
| customers | `CUS_CITY_NOT_NULL` | city | blank | Completeness | Warning | 6 | C00355 |
| customers | `CUS_PINCODE_6_DIGITS` | pincode | `5600XX` | Validity | Warning | 3 | C00132 |
| customers | `CUS_SIGNUP_DATE_NOT_FUTURE` | signup_date | 2026-11/12, 2027 | Timeliness | Warning | 3 | C00400 |
| products | `PRD_PRODUCT_ID_UNIQUE` | product_id | stale "(old)" row | Uniqueness | Critical | 2 | P0030, P0140 |
| products | `PRD_COST_LE_MRP` | cost_price, mrp | cost > MRP (×1.2) | Accuracy | Warning | 4 | P0019, P0073 |
| products | `PRD_CATEGORY_NOT_NULL` | category | blank | Completeness | Warning | 3 | P0051 |
| products | `PRD_MRP_POSITIVE` | mrp | −203 | Accuracy | Warning | 1 | P0109 |
| stores | `STO_ZONE_NOT_NULL` | zone | blank (S07) | Completeness | Warning | 1 | S07 |
| stores | `STO_CAPACITY_POSITIVE` | capacity_orders_per_day | 0 | Accuracy | Warning | 1 | S12 |

### 1.5 Three questions I would ask the client before building

1. **Is a zero or negative quantity a return or a data-entry error?** 10 DELIVERED/CANCELLED orders have quantity 0, −1, −2 or −3 with matching negative amounts (and gateway payments of 0 or negative value). I quarantined them; if they are real returns they belong in a returns feed, not in orders.
2. **Which zone is store S07 (BTM Layout) in — and should an order be counted when its store, product or customer master record is incomplete?** S07's blank zone, three products with no category and four with cost > MRP touch about **₹1.7 lakh (≈14%) of delivered revenue**. I kept those orders and bucketed the unknown attribute as `UNKNOWN` (visible, not silent); finance may prefer to hold them until the master is fixed.
3. **When an order is RETURNED after the month closed, should September be restated or should the return reduce the month it happens in?** The increment flips 10 September orders (₹4,193.70) to RETURNED, and the gateway still shows those payments as SUCCESS (no refund issued). I built both views (§5 Q9) but need Finance's choice for the dashboard default.

### 1.6 What profiling changed in the design (severity policy)

The assignment suggests Critical for e.g. "category not null in products". I measured the blast radius before accepting that:

| If quarantined instead of flagged… | Delivered orders lost | Revenue lost |
|---|---|---|
| store S07 (blank zone) | 150 | ₹93,674 (**7.7 %**) |
| store S12 (capacity 0) | 122 | ₹64,348 (5.3 %) |
| 3 products with blank category | 43 | ₹38,119 (3.1 %) |
| 5 products with bad cost/MRP | 72 | ₹35,989 (3.0 %) |
| 3 customers with a future signup date | 17 | ₹10,782 (0.9 %) |

Quarantining a *dimension* row for a *descriptive* defect would silently delete valid revenue from the CFO's total (and orphan the fact rows). So:
**Fact rows (orders, payments) follow the suggested severities. Dimension rows are quarantined only for key problems (blank or duplicate key);
a missing or odd attribute is a Warning — repaired where a safe fix exists (`UNKNOWN`, `NULL`), flagged, and counted in `dq_results`.**
This is a deliberate deviation from the suggested table and is the reason the ETL and ELT numbers can both be defended in front of Finance.

---

## 2. Parts A & B — design notes

```
ETL   5 CSV ─► extract ×5 (parallel) ─► validate (pandas, 6 dimensions) ─┬─► quarantine (CSV + reason, PII masked)
                                                                          └─► transform (standardise · dedupe · MASK · join · derive)
                                                                                 ─► load star schema (MERGE) ─► reconcile ─► report ─► audit
ELT   5 CSV ─► extract ×5 (locate · header check · SHA-256) ─► raw_* (text, append-only) ─► stg_* (SQL: cast · mask · dedupe · DQ)
                                                          └─ increment ─► raw (new batch) ─► MERGE into stg_orders ─► mart_* (SQL) ─► audit
```

| Topic | ETL (`dags/nammamart_etl_sudarshan.py`) | ELT (`dags/nammamart_elt_sudarshan.py`) |
|---|---|---|
| Parallelism | 5 extract tasks start together (+0.0 s); the 3 dimension validations then run in parallel | 5 extracts, then 5 raw loads, then 3 dimension stagings, then 8 mart tasks |
| Where the rules live | `include/nammamart/etl/validate.py` (pandas) | `include/sql/stg_*.sql` (SQL arrays `dq_critical` / `dq_warnings`) |
| Rejected rows | `include/quarantine/sudarshan/<file>_failed_<run_id>.csv` + `etl_quarantine` | stay in `stg_*` with `dq_status='FAIL'` + `failure_reason`; raw keeps them untouched |
| Idempotency | `MERGE … ON order_id` with a row hash: same data ⇒ 0 inserts, 0 updates | raw: identical file skipped by SHA-256; `stg_orders` MERGE on `order_id`; staging/marts rebuilt deterministically |
| Increment | extract picks up `orders_*_increment.csv`; newer file supersedes the older row; MERGE updates | raw appends a new batch; staging picks the newest batch per `order_id`; MERGE updates |
| History of changes | `etl_order_changes` | `stg_order_changes` + both versions stay in `raw_orders` |
| Run audit + verdict | `write_run_audit_row` (`trigger_rule=all_done`) → `pipeline_runs`; then `run_verdict` (the only leaf) fails the **run** if any task failed | same |
| Lineage columns | fact carries `source_file`, `source_row_num`, `etl_run_id` | every layer carries `_source_file`, `_source_row_num`, `_batch_id`, `_run_id`, `_loaded_at` |

Cross-cutting engineering choices that mattered in testing:
* **One rulebook, two implementations.** `include/nammamart/dq_catalog.py` lists all 66 checks (name, dimension, severity). ETL implements them in pandas, ELT in SQL; `test_parity.py`
  proves **every check gives the same `rows_failed`**, every rejected row has the same reason, the dimensions (including masked PII) are identical, and the marts equal the ETL queries.
  That is what makes "mart totals match ETL totals for September" a guarantee rather than a hope. **They match exactly; there is nothing to explain** — the only legitimate difference arises *after* the increment (both then agree with each other again, 2,458 orders).
* **DuckDB has one writer.** Parallel Airflow tasks (and two DAGs at 09:00) are separate processes, so `connect()` retries on the file lock instead of failing. Observed under real concurrency: tasks queued behind each other (e.g. `load_raw_payments` waited 13 s) and none failed. `max_active_runs=1`.
* **Schema contract.** Extract compares the CSV header with the agreed columns. A *missing file* is retried (it may simply not have arrived); a *changed schema* fails immediately (`AirflowFailException`, no retries — retrying a deterministic error just burns the 9 AM window).
* **Idempotent audit/DQ tables.** `dq_results` and `pipeline_runs` are replaced per `(run_id, dag_id)`, so a retried or cleared task never double-writes.
* **Reconciliation compares clean orders with *all* gateway payments** (only the extra copies of a duplicated `payment_id` are dropped): the gateway is the source of truth for money received, so anomalies must be *shown*. This surfaced a finding Finance has been chasing by hand: **90 payments (₹84,813) are for orders that failed validation** and therefore are not in revenue.

---

## 3. Part C — Scheduling

```python
@dag(
    schedule=CronTriggerTimetable("0 9 * * 1-6", timezone="Asia/Kolkata"),   # 09:00 IST, Mon-Sat
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),             # timezone-aware
    catchup=False, max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=60), "execution_timeout": timedelta(minutes=10), ...},
)
```

**What is your cron expression?** `0 9 * * 1-6` — minute 0, hour 9, any day, any month, days-of-week 1–6 (Mon–Sat; Sunday has no run). Airflow describes it as *"At 09:00, Monday through Saturday"*.

**What time does the run show in UTC in the Airflow UI?** **03:30 UTC.** India is UTC+05:30 all year (no daylight saving). Verified: `airflow dags next-execution` lists `2026-10-06T03:30:00+00:00`, `2026-10-07T03:30:00+00:00`, … and the scheduler's run id was `scheduled__2026-10-06T03:30:00+00:00`.
(The UI shows times in the browser's timezone by default; the run is stored in UTC.)

**What would happen at 9 AM if the `start_date` had been left in UTC?** Airflow takes the schedule's timezone from the DAG's `start_date`/timetable. In UTC, `0 9 * * 1-6` means **09:00 UTC = 14:30 IST — 5½ hours late and after the 9:30 stand-up**. The SLA would be missed every single day while every run looked "green". (The weekday boundary also moves: a Saturday 09:00 IST slot is still Saturday, but anything computed around midnight IST would be a day off.)

**What happens if my laptop or Docker is off at 9 AM?** Nothing runs — the scheduler is the thing that starts the DAG. I read the timetable source and then saw it happen: with `catchup=False` the scheduler creates **one** run for the **most recent missed tick** as soon as it is running again, and **does not backfill** earlier days. When I un-paused the DAGs at 16:38 UTC, Airflow immediately created `scheduled__2026-10-06T03:30:00+00:00` (today's 09:00 slot, ~13 h late) and ran it to success. So: data arrives late by however long Docker was off, but never 30 duplicate runs. For a real deployment the scheduler must live on an always-on host (an Astro Deployment, not a laptop), and a deadline alert should fire if the 09:30 SLA is at risk.

**Why `catchup=False`?** Each run re-reads the *full* snapshot and is idempotent (MERGE, hash-skip). A backfill of 30 missed days would just replay the same files 30 times — extra work and extra `pipeline_runs`/`dq_results` rows with the wrong "now" — and produce no new information. The one latest run is the only run that matters. (If NammaMart later delivers date-partitioned files, that changes: use `catchup=True` with partition-aware loads.)

**Retries / timeout justification.** `retries=2, retry_delay=60 s` — a locked file or slow disk is transient; 3 attempts fit inside ≈3 minutes, still far ahead of 09:30. `execution_timeout=10 min` — a hung task must fail loudly. Failures call `notify_failure` (Slack through the `SLACK_WEBHOOK_URL` Variable, logged if unset).
Observed (Exp 4): a missing `products.csv` produced attempts at 17:12:19 → 17:13:21 → 17:14:23 and then `failed`, ≈ 62 s apart.

---

## 4. Part D — Data-quality dimensions

66 checks, implemented in **both** pipelines, every one with a severity. **Every check writes one row to `dq_results` on every run — passes included** (66 rows per DAG per run; a failed *Critical* check shows `status = FAIL`, a failed *Warning* shows `WARN`).

| Dimension | Critical | Warning | Examples (check → what it caught) |
|---|---|---|---|
| Completeness | 9 | 5 | `ORD_CUSTOMER_ID_NOT_NULL` (18), `ORD_DELIVERED_HAS_MINUTES` (8), `PRD_CATEGORY_NOT_NULL` (3, warning), `BATCH_ORDERS_VOLUME` |
| Uniqueness | 5 | 0 | `ORD_ORDER_ID_UNIQUE` (12), `PAY_PAYMENT_ID_UNIQUE` (4), `CUS_CUSTOMER_ID_UNIQUE` (5), `PRD_PRODUCT_ID_UNIQUE` (2, keeps the current row, rejects "(old)") |
| Validity | 9 | 12 | `ORD_STATUS_VALID` (3 `Pending??`), `ORD_ORDER_TS_VALID` (5), `ORD_DISCOUNT_RANGE` (3), `CUS_EMAIL_FORMAT` (12, warning), `CUS_PHONE_10_DIGITS` (7, warning) |
| Accuracy | 6 | 7 | `ORD_AMOUNT_MATCHES_FORMULA` (18), `ORD_QUANTITY_POSITIVE` (10), `ORD_DELIVERY_MINUTES_MAX` (6), `ORD_PRICE_FAR_FROM_CATALOGUE` (added after Exp 8b), `PRD_COST_LE_MRP` (4, warning), `BATCH_STORE_REJECT_RATE` |
| Consistency | 4 | 3 | `ORD_PRODUCT_EXISTS` (9), `ORD_STORE_EXISTS` (5), `PAY_ORDER_EXISTS` (8), `CUS_LOYALTY_TIER_CASING` (8, warning), `PAY_AMOUNT_MATCHES_ORDER` (14, warning) |
| Timeliness | 2 | 4 | `ORD_ORDER_TS_NOT_FUTURE` (7), `PAY_TS_NOT_FUTURE` (7), `BATCH_ORDERS_FRESH` (latest order > 1 day old at run time) |

* **Critical → quarantined / `FAIL`** (never reaches reporting). **Warning → loaded, flagged, counted** (e.g. `UPI ` is mapped to `UPI` and counted under `ORD_PAYMENT_MODE_STANDARDISED`).
* **Not guessed:** `upi`/`CARD`/`cod` are mapped through an explicit alias list; `Cashh` is *not* "probably cash" — it is rejected (and listed for the client).
* **Where the checks run:** ETL — `include/nammamart/etl/validate.py` (Python); ELT — `include/sql/stg_*.sql` + `include/sql/elt_dq_results.sql` (SQL). Both are driven by `include/nammamart/dq_catalog.py`.
* **Sample of the scorecard** (`SELECT check_name, dimension, severity, rows_checked, rows_failed, status FROM dq_results WHERE dag_id = 'nammamart_elt_sudarshan' AND rows_failed > 0`):

| check_name | dimension | severity | rows_checked | rows_failed | status |
|---|---|---|---|---|---|
| ORD_AMOUNT_MATCHES_FORMULA | Accuracy | Critical | 2,562 | 18 | FAIL |
| ORD_CUSTOMER_ID_NOT_NULL | Completeness | Critical | 2,562 | 18 | FAIL |
| ORD_ORDER_ID_UNIQUE | Uniqueness | Critical | 2,562 | 12 | FAIL |
| PAY_AMOUNT_MATCHES_ORDER | Consistency | Warning | 2,228 | 14 | WARN |
| CUS_EMAIL_FORMAT | Validity | Warning | 405 | 12 | WARN |
| BATCH_ORDERS_FRESH | Timeliness | Warning | 1 | 1 | WARN |

(`rows_checked` is 2,562 here = 2,412 base rows − 10 superseded by corrections + 160 increment rows; it is 2,412 in the September-only experiments.)
`BATCH_ORDERS_FRESH` warns on every run of this demo for a legitimate reason: the data is static and its newest valid order (2026-10-01) is older than a day. In production it would only warn when the 9 AM export is stale.

**Severity deviations from the suggested table** (all quantified in §1.6): `PRD_CATEGORY_NOT_NULL`, `STO_ZONE_NOT_NULL`, `PRD_COST_LE_MRP`/`PRD_MRP_POSITIVE`, `CUS_SIGNUP_DATE_NOT_FUTURE` are **Warning** rather than Critical because quarantining a dimension row would delete or orphan valid orders.

---

## 5. Part E — Data governance

*(Answers are specific to this pipeline. Where law is involved, treat it as an engineer's reading to be confirmed by NammaMart's counsel / DPO.)*

**1. Ownership.**

| Dataset | Business owner (accountable) | Steward (day-to-day) |
|---|---|---|
| `orders` | Head of Operations (COO's org) — source: order-management system | Ops analyst |
| `payments` | Finance (CFO's org) — source: payment gateway | Finance controller / reconciliation analyst |
| `customers` | CRM / Marketing head; DPO oversees personal-data use | CRM analyst |
| `products` | Category / merchandising head | Catalogue manager |
| `stores` | Head of Store Network | Zone managers |
| Warehouse: `raw_*`, `stg_*`, `etl_*`, `dq_*`, `pipeline_runs` | CTO (Sameer) — data platform | FDE / platform engineer |
| Warehouse: marts and `etl_fact_orders` | The metric's business owner (revenue & reconciliation → Finance; delivery KPIs → Ops) | analytics owner |
| Quarantine files | Data steward of the *source* system (so the fix happens at the source) | Ops analyst |

**Who approves a change to a quality rule** such as the 120-minute delivery limit? The **metric owner (Head of Operations)** decides the business value; the **data platform owner (CTO's team)** reviews it as a code change (`dq_catalog.py` + the ETL/ELT implementation, via pull request, with the parity tests green); **Finance** co-approves any rule that changes revenue (amount, status, duplicate handling). The rule's severity change is recorded in git history.

**2. Data dictionary** (ETL star schema; the ELT `stg_*` tables carry the same business columns plus `dq_critical`, `dq_warnings`, `dq_status`, `failure_reason`, `_source_file`, `_source_row_num`, `_batch_id`, `_run_id`, `_loaded_at`).

`etl_fact_orders` — grain: one row per clean order

| Column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| order_id (PK) | VARCHAR | order identifier | orders.order_id | `ORD######`, unique | No |
| order_ts / order_date | TIMESTAMP / DATE | when the order was placed (IST) | orders.order_ts | not in the future | No |
| customer_id | VARCHAR | customer key | orders.customer_id | exists in `etl_dim_customer` | **Indirect** (pseudonymous ID, linkable to a person) |
| store_id, product_id | VARCHAR | store / product keys | orders | exist in dims | No |
| quantity | INTEGER | units | orders | > 0 | No |
| unit_price, discount_pct | DECIMAL | list price; discount % | orders | price > 0; 0–100 | No |
| order_amount | DECIMAL(12,2) | `qty × price × (1 − disc/100)` | orders | equals the formula | No |
| payment_mode | VARCHAR | standardised | orders | UPI / Card / Cash on Delivery / Wallet | No |
| order_status | VARCHAR | standardised | orders | DELIVERED / CANCELLED / RETURNED | No |
| channel | VARCHAR | where ordered | orders | App / Web | No |
| delivery_minutes | INTEGER | minutes to deliver | orders | required if DELIVERED; ≤ 120 | No |
| net_revenue | DECIMAL(12,2) | `order_amount` if DELIVERED, else 0 | derived | ≥ 0 | No |
| gross_margin | DECIMAL(12,2) | `net_revenue − qty × cost_price` (DELIVERED); NULL if cost unknown | derived (products.cost_price) | — | No |
| within_15_min | BOOLEAN | delivered in ≤ 15 min; NULL unless DELIVERED | derived | — | No |
| row_hash, source_file, source_row_num, etl_run_id, loaded_at, updated_at | — | lineage and change detection | pipeline | — | No |

`etl_dim_customer` — one row per customer; **no raw name, email or phone column exists**

| Column | Type | Meaning | Allowed values | PII |
|---|---|---|---|---|
| customer_id (PK) | VARCHAR | key | `C#####` | Indirect |
| customer_name_masked | VARCHAR | initials, e.g. `P. H.` | — | masked (derived from a name) |
| email_masked | VARCHAR | e.g. `pr***@example.com`; NULL if `N/A` | — | masked |
| phone_masked | VARCHAR | e.g. `97******21` | — | masked |
| city, area, pincode | VARCHAR | home location (`UNKNOWN` / NULL if missing or invalid) | pincode = 6 digits | **Indirect** (location) |
| signup_date | DATE | NULL if in the future | — | No |
| loyalty_tier | VARCHAR | one casing | Bronze / Silver / Gold / Platinum / UNKNOWN | No |
| is_active | VARCHAR | | Y / N | No |
| dq_flags | VARCHAR | warnings raised on this row | — | No |

`etl_dim_product`: product_id (PK), product_name, **category** (`UNKNOWN` if blank), sub_category, brand, mrp (NULL if ≤ 0), cost_price, unit, is_perishable (BOOLEAN), launch_date, dq_flags — PII: none.
`etl_dim_store`: store_id (PK), store_name, **zone** (North/South/East/West/`UNKNOWN`), area, pincode, open_date, **manager_name** (employee personal data — internal use), capacity_orders_per_day (NULL if ≤ 0), dq_flags.

**3. Lineage — one number traced to its source rows** (full output: `submission/results/lineage_audit_change.md`). `mart_daily_store_kpis`, store **S05**, **2026-09-01** → `net_revenue = 803.00` (2 delivered orders):

* **ELT:** `mart_daily_store_kpis` → `v_orders_clean` → `stg_orders` rows `ORD000049` (₹11.00) and `ORD000062` (₹792.00), which carry `_source_file = orders.csv`, `_batch_id`, `_source_row_num = 352 / 1977` → the identical lines in `raw_orders`, as received. Every hop is a SQL statement in `include/sql/`.
* **ETL:** `etl_daily_zone_revenue` → `etl_fact_orders` rows with `source_file = orders.csv`, `source_row_num = 352 / 1977`, `etl_run_id` → the file.

**Which is easier, and why?** **ELT.** The raw layer is the audit trail: the whole chain is SQL, every layer keeps the file/line/batch, and *rejected* rows are still there for the same trace. In ETL the accepted rows can be traced (I carry `source_row_num`), but a *rejected* row exists only in a per-run quarantine file, and what the extract read is not stored anywhere in the warehouse — proving a negative ("it isn't in revenue") means searching files.

**4. Personal data.**
Columns that are personal data in this dataset: `customers.full_name`, `email`, `phone` (**direct identifiers**); `area`, `pincode` (and, weakly, `city`) — home location, **indirect**; `customer_id` — pseudonymous key that links orders, payments and behaviour to a person, so also personal data; `payments.gateway_ref` (a transaction reference linkable to a person via the order); `stores.manager_name` (employee personal data). `signup_date`, `loyalty_tier` and order history are personal data once linked to the person.
**How masked:** in the ETL transform (and in ELT staging SQL — identical rules, proven by a parity test) *before* anything reaches a reporting table: name → initials, email → first 2 characters + `***` + domain, phone → first 2 + last 2 digits. Malformed-but-present values (a 9-digit phone) are still masked; `N/A` has nothing to protect and becomes NULL. Quarantine files for customers are masked too.
**Why mask rather than delete?** Deleting the columns would break legitimate use: joins and counts by customer, top-customer lists, support teams recognising "the customer ending 21", de-duplication. Masking is data minimisation + pseudonymisation: the business keeps the analytical value, and the raw values stay only in the restricted raw layer. (Deletion is the right answer when the *purpose* ends — see Q6.)
**Law:** India's **Digital Personal Data Protection Act, 2023 (DPDP Act)** and the Rules made under it apply to NammaMart as a *data fiduciary* processing digital personal data of customers in India. In practice it expects: a clear purpose and notice/consent for each use; only the data necessary for that purpose; accuracy; **reasonable security safeguards** (masking, access control, encryption); **storage limitation** (erase when the purpose is served or consent is withdrawn); **breach notification** to the Data Protection Board and affected people; honouring access/correction/erasure and grievance requests; extra care for children's data. (Confirm commencement dates and any sector rules with counsel.)

**5. Access control.**

| Layer | Who may read | Why |
|---|---|---|
| `raw_*` (ELT) | Data-platform engineers only; read-only; break-glass for audit | exact copy incl. bad rows and **raw PII** |
| `stg_*` / `etl_*` dims and fact | Data platform + analysts | masked, but row-level and includes rejected rows (staging) |
| marts / `etl_fact_orders`, reports | Business users: Finance, Ops, COO (read-only) | no PII, only business-ready numbers |
| quarantine files & `etl_quarantine` | Data stewards of the source system + platform | contains row-level data to fix at source (customers masked) |
| `dq_results`, `pipeline_runs` | Everyone in data/ops | no row data; operational transparency |

**Raw `customers` holds unmasked phone numbers — who, if anyone, should see it?** By default **nobody**. A named engineer, with DPO approval, time-boxed and logged, only for an incident or a legal/audit request; marketing outreach goes through the CRM (which has its own consent records), not the warehouse. *Honest limit:* a DuckDB file has no roles — in production the raw layer must move to a warehouse with RBAC and encryption, or the raw customers table must be tokenised at load.

**6. Retention.**

| Data | Keep | After that |
|---|---|---|
| Raw orders / payments (financial records) | ≈ 8 years (books-of-account / tax record-keeping; Finance to confirm) | archive then delete |
| Raw customers (raw PII snapshots) | latest snapshot while the customer is active; older snapshots ≤ 12 months | purge snapshots; on consent withdrawal / erasure, delete or irreversibly anonymise (and propagate to staging) |
| Quarantine files | 90 days, or until the source error is fixed and the file re-processed | delete (the *counts* stay forever in `dq_results` / `mart_dq_rejections`) |
| Masked customer data (dims, staging) | while the customer relationship lasts + ≈ 1 year of inactivity | delete or aggregate; keep only non-identifying aggregates |
| `dq_results`, `pipeline_runs` | indefinitely (small, no PII) | — |
A legal hold overrides every deletion. Deletions are themselves logged.

**7. Secrets.** `SLACK_WEBHOOK_URL` is an **Airflow Variable** (Admin → Variables, or `airflow_settings.yaml` / an `AIRFLOW_VAR_…` environment variable, both git-ignored; on a real deployment a secrets backend). `notify_failure()` reads it with `Variable.get("SLACK_WEBHOOK_URL")` at the moment of the alert — it is never a string in a DAG file. It must not be in the DAG or on GitHub because a repository is permanent and widely readable (history keeps a "deleted" secret), a leaked webhook lets anyone post to the company's Slack (phishing, fake alerts), and rotating it would otherwise need a code change and redeploy. `.gitignore` excludes `airflow_settings.yaml` and `.env`; this repo contains no keys, passwords or webhook URLs.

**8. Audit — "pick any rejected order; show it exactly as received, the rule it broke, and prove it isn't in the CFO's total"** (real output: `submission/results/lineage_audit_change.md`). Order **`ORD001296`**:

| Step | ELT | ETL |
|---|---|---|
| As received | `raw_orders` line 800 of `orders.csv`: `quantity=-2, order_amount=-394.0, DELIVERED` (batch, run id, load time attached) | quarantine file `orders_failed_<run_id>.csv`, `source_row_num=800`, all 13 original columns |
| Rule it broke | `stg_orders.failure_reason = ORD_QUANTITY_POSITIVE`, `dq_status = FAIL` | `etl_quarantine.failure_reason = ORD_QUANTITY_POSITIVE` and the `failure_reason` column in the file |
| Not in revenue | `SELECT count(*) FROM v_orders_clean WHERE order_id='ORD001296'` → **0** (every mart reads `v_orders_clean`); mart total ₹12,10,421.30 = sum of clean delivered | `SELECT count(*) FROM etl_fact_orders WHERE order_id='ORD001296'` → **0** |
ELT answers it from one place with SQL; ETL answers it from the quarantine file plus the fact table.

**9. Change history — order DELIVERED on 3 September, RETURNED on 1 October.**
After the increment **both** pipelines overwrite the order and so show **September revenue restated**: ₹11,34,962.80 → **₹11,30,769.10** (−₹4,193.70 for the 10 corrected orders).
**Which does Finance want?** Usually *both*: books for September were closed and reported at ₹11,34,962.80, so Finance generally wants September left **as reported** and the return recognised **in October** (as a reduction), while Ops wants the restated truth. **How both views are kept:** `etl_order_changes` / `stg_order_changes` record every overwrite (order, old and new status/amount, run, file), and the ELT raw layer keeps both versions of the row. As-reported September = restated September + `sum(old_net_revenue − new_net_revenue)` over the change rows (query in `submission/trace_examples.py`); October's return view = the same change rows grouped by `changed_at`.
Evidence from the real base-then-increment sequence: `submission/results/lineage_audit_change.md` (e.g. `ORD000337`: both versions in `raw_orders`, one `DELIVERED → RETURNED` row in `etl_order_changes` and `stg_order_changes`; September restated ₹11,30,769.10 vs as originally reported ₹11,34,962.80). A related finding: the gateway still shows those 10 payments as **SUCCESS** — `RETURNED_NOT_REFUNDED`, ₹4,193.70 — which the reconciliation flags for the refund team.

---

## 6. The six business questions

Each is **one simple query** on the final tables. Queries: `submission/queries/business_questions_etl.sql` and `…_elt.sql`; run with
`python submission/run_business_questions.py --warehouse include/warehouse_sudarshan.duckdb --pipeline etl|elt`.
Full results (all rows): `submission/results/business_questions_etl_results.md`, `submission/results/business_questions_elt_results.md`.
Numbers below are the production state (September + the 2026-10-01 increment) and are **identical from both pipelines**.

### Q1a - Daily net revenue by ZONE (delivered orders only, after discount)

**ETL SQL** (`etl_*` tables)

```sql
SELECT order_date, zone, sum(delivered_orders) AS orders, round(sum(net_revenue), 2) AS net_revenue
FROM etl_daily_zone_revenue
GROUP BY order_date, zone
ORDER BY order_date, zone;
```

**ELT SQL** (`mart_*` tables)

```sql
SELECT order_date, zone, sum(delivered_orders) AS orders, round(sum(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
WHERE delivered_orders > 0
GROUP BY order_date, zone
ORDER BY order_date, zone;
```

| order_date | zone | orders | net_revenue |
|---|---|---|---|
| 2026-09-01 | East | 19 | 10192.85 |
| 2026-09-01 | North | 16 | 9217.15 |
| 2026-09-01 | South | 9 | 4718.30 |
| 2026-09-01 | UNKNOWN | 9 | 5844.75 |
| 2026-09-01 | West | 14 | 6563.65 |
| 2026-09-02 | East | 17 | 8737.90 |
| 2026-09-02 | North | 9 | 3573.80 |
| 2026-09-02 | South | 18 | 14038.00 |


_147 more rows in the full result file (155 rows)._

**ETL result vs ELT result: IDENTICAL** (155 rows compared). Revenue is bucketed `UNKNOWN` for store S07 (blank zone in the master) instead of being dropped.

### Q1b - Daily net revenue by STORE

**ETL SQL** (`etl_*` tables)

```sql
SELECT order_date, store_id, store_name, delivered_orders, net_revenue
FROM etl_daily_zone_revenue
ORDER BY order_date, store_id;
```

**ELT SQL** (`mart_*` tables)

```sql
SELECT order_date, store_id, store_name, delivered_orders, net_revenue
FROM mart_daily_store_kpis
WHERE delivered_orders > 0
ORDER BY order_date, store_id;
```

| order_date | store_id | store_name | delivered_orders | net_revenue |
|---|---|---|---|---|
| 2026-09-01 | S01 | NammaMart Hebbal | 4 | 2247.80 |
| 2026-09-01 | S02 | NammaMart Yelahanka | 6 | 2385.55 |
| 2026-09-01 | S03 | NammaMart RT Nagar | 6 | 4583.80 |
| 2026-09-01 | S04 | NammaMart Jayanagar | 1 | 1103.30 |
| 2026-09-01 | S05 | NammaMart JP Nagar | 2 | 803.00 |
| 2026-09-01 | S06 | NammaMart Banashankari | 6 | 2812.00 |
| 2026-09-01 | S07 | NammaMart BTM Layout | 9 | 5844.75 |
| 2026-09-01 | S08 | NammaMart Whitefield | 4 | 2984.70 |


_192 more rows in the full result file (200 rows)._

**ETL result vs ELT result: IDENTICAL** (200 rows compared).

### Q2 - Revenue and gross margin by product category (margin = revenue - quantity x cost_price)

**ETL SQL** (`etl_*` tables)

```sql
SELECT category, delivered_orders, units_sold, revenue, cost_of_goods, gross_margin, margin_pct
FROM etl_category_margin;
```

**ELT SQL** (`mart_*` tables)

```sql
SELECT category, delivered_orders, units_sold, revenue, cost_of_goods, gross_margin, margin_pct
FROM mart_category_margin;
```

| category | delivered_orders | units_sold | revenue | cost_of_goods | gross_margin | margin_pct |
|---|---|---|---|---|---|---|
| Staples | 336 | 624 | 287601.45 | 225423.36 | 62178.09 | 21.62 |
| Personal Care | 347 | 678 | 215345.30 | 168422.72 | 46922.58 | 21.79 |
| Household | 362 | 664 | 211727.05 | 157374.54 | 54352.51 | 25.67 |
| Dairy & Bakery | 405 | 760 | 179568.95 | 131935.95 | 47633.00 | 26.53 |
| Snacks & Beverages | 340 | 682 | 174700.60 | 133329.82 | 41370.78 | 23.68 |
| Fruits & Vegetables | 392 | 751 | 99566.20 | 79054.82 | 20511.38 | 20.6 |
| UNKNOWN | 44 | 88 | 41911.75 | 36968.36 | 4943.39 | 11.79 |


**ETL result vs ELT result: IDENTICAL** (7 rows compared). `UNKNOWN` = the 3 products with a blank category (their revenue is kept, visibly).

### Q3 - Average delivery time per store and % of delivered orders within 15 minutes

**ETL SQL** (`etl_*` tables)

```sql
SELECT s.store_id, s.store_name,
       count(*)                                                       AS delivered_orders,
       round(avg(f.delivery_minutes), 2)                              AS avg_delivery_minutes,
       round(100.0 * avg(CASE WHEN f.within_15_min THEN 1 ELSE 0 END), 2) AS pct_within_15_min
FROM etl_fact_orders f
JOIN etl_dim_store s ON s.store_id = f.store_id
WHERE f.order_status = 'DELIVERED' AND f.delivery_minutes IS NOT NULL
GROUP BY s.store_id, s.store_name
ORDER BY s.store_id;
```

**ELT SQL** (`mart_*` tables)

```sql
SELECT store_id, store_name, delivered_orders, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla
ORDER BY store_id;
```

| store_id | store_name | delivered_orders | avg_delivery_minutes | pct_within_15_min |
|---|---|---|---|---|
| S01 | NammaMart Hebbal | 149 | 20.47 | 32.21 |
| S02 | NammaMart Yelahanka | 150 | 20.93 | 31.33 |
| S03 | NammaMart RT Nagar | 143 | 20.65 | 29.37 |
| S04 | NammaMart Jayanagar | 138 | 21.31 | 27.54 |
| S05 | NammaMart JP Nagar | 142 | 21.19 | 29.58 |
| S06 | NammaMart Banashankari | 153 | 22.52 | 21.57 |
| S07 | NammaMart BTM Layout | 150 | 21.13 | 32.67 |
| S08 | NammaMart Whitefield | 164 | 20.61 | 35.37 |
| S09 | NammaMart Indiranagar | 136 | 21.13 | 26.47 |
| S10 | NammaMart Marathahalli | 150 | 22.09 | 26.0 |
| S11 | NammaMart HSR Layout | 160 | 20.88 | 28.13 |
| S12 | NammaMart Rajajinagar | 118 | 21.86 | 24.58 |
| S13 | NammaMart Malleshwaram | 150 | 21.87 | 27.33 |
| S14 | NammaMart Vijayanagar | 166 | 21.51 | 31.33 |
| S15 | NammaMart Basaveshwaranagar | 157 | 22.03 | 22.93 |


**ETL result vs ELT result: IDENTICAL** (15 rows compared). Orders rejected for impossible delivery times (> 120 min) are already excluded, so they no longer distort the 10-minute promise.

### Q4a - Revenue by payment mode

**ETL SQL** (`etl_*` tables)

```sql
SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY payment_mode
ORDER BY net_revenue DESC;
```

**ELT SQL** (`mart_*` tables)

```sql
SELECT payment_mode, delivered_orders, net_revenue, pct_of_revenue
FROM mart_revenue_by_payment_mode;
```

| payment_mode | delivered_orders | net_revenue | pct_of_revenue |
|---|---|---|---|
| UPI | 1205 | 638752.15 | 52.77 |
| Card | 457 | 251882.30 | 20.81 |
| Cash on Delivery | 318 | 181668.40 | 15.01 |
| Wallet | 246 | 138118.45 | 11.41 |


**ETL result vs ELT result: IDENTICAL** (4 rows compared).

### Q4b - Amount that does not reconcile with the payment gateway (by reason; feed-pending is a timing difference)

**ETL SQL** (`etl_*` tables)

```sql
SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS unreconciled_amount
FROM etl_payment_reconciliation
GROUP BY mismatch_reason
UNION ALL
SELECT 'TOTAL (excl. PAYMENT_FEED_PENDING)', count(*), round(sum(unreconciled_amount), 2)
FROM etl_payment_reconciliation WHERE mismatch_reason <> 'PAYMENT_FEED_PENDING'
ORDER BY 3 DESC;
```

**ELT SQL** (`mart_*` tables)

```sql
SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS unreconciled_amount
FROM mart_payment_reconciliation
GROUP BY mismatch_reason
UNION ALL
SELECT 'TOTAL (excl. PAYMENT_FEED_PENDING)', count(*), round(sum(unreconciled_amount), 2)
FROM mart_payment_reconciliation WHERE mismatch_reason <> 'PAYMENT_FEED_PENDING'
ORDER BY 3 DESC;
```

| mismatch_reason | rows | unreconciled_amount |
|---|---|---|
| TOTAL (excl. PAYMENT_FEED_PENDING) | 191 | 137545.85 |
| PAYMENT_FOR_REJECTED_ORDER | 90 | 84812.98 |
| PAYMENT_FEED_PENDING | 150 | 79652.20 |
| PAYMENT_FAILED | 42 | 25722.25 |
| MISSING_PAYMENT | 28 | 17476.35 |
| ORPHAN_PAYMENT | 8 | 4580.57 |
| RETURNED_NOT_REFUNDED | 10 | 4193.70 |
| AMOUNT_MISMATCH | 13 | 760.00 |


**ETL result vs ELT result: IDENTICAL** (8 rows compared). `PAYMENT_FEED_PENDING` is a timing difference (the 150 orders of 1 Oct are newer than the newest payment in the gateway file); the TOTAL row excludes it.

### Q5 - Top 10 customers by spend, with loyalty tier (masked identity only)

**ETL SQL** (`etl_*` tables)

```sql
SELECT rank() OVER (ORDER BY sum(f.net_revenue) DESC) AS spend_rank,
       f.customer_id, c.customer_name_masked, c.loyalty_tier,
       count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS net_spend
FROM etl_fact_orders f
JOIN etl_dim_customer c ON c.customer_id = f.customer_id
WHERE f.order_status = 'DELIVERED'
GROUP BY f.customer_id, c.customer_name_masked, c.loyalty_tier
ORDER BY spend_rank, f.customer_id
LIMIT 10;
```

**ELT SQL** (`mart_*` tables)

```sql
SELECT spend_rank, customer_id, customer_name_masked, loyalty_tier, delivered_orders, net_spend
FROM mart_top_customers
WHERE spend_rank <= 10
ORDER BY spend_rank, customer_id;
```

| spend_rank | customer_id | customer_name_masked | loyalty_tier | delivered_orders | net_spend |
|---|---|---|---|---|---|
| 1 | C00136 | S. B. | Bronze | 9 | 8738.10 |
| 2 | C00115 | S. K. | Bronze | 8 | 8037.70 |
| 3 | C00283 | N. R. | Bronze | 8 | 7934.60 |
| 4 | C00236 | S. N. | Platinum | 12 | 7831.80 |
| 5 | C00310 | K. P. | Bronze | 8 | 7258.95 |
| 6 | C00265 | V. H. | Gold | 8 | 7151.30 |
| 7 | C00071 | M. P. | Silver | 11 | 6933.30 |
| 8 | C00338 | B. I. | Silver | 9 | 6848.55 |
| 9 | C00357 | A. N. | Bronze | 11 | 6637.90 |
| 10 | C00209 | K. P. | Platinum | 8 | 6519.40 |


**ETL result vs ELT result: IDENTICAL** (10 rows compared). Only masked identity (initials) and tier are shown - no raw name, email or phone.

### Q6 - Rows rejected in the latest run, by file and by reason

**ETL SQL** (`etl_*` tables)

```sql
SELECT source_file, reason AS failure_reason, count(*) AS rows_rejected
FROM (
    SELECT source_file, unnest(string_split(failure_reason, '; ')) AS reason
    FROM etl_quarantine
    WHERE run_id = (SELECT max_by(run_id, quarantined_at) FROM etl_quarantine)
)
GROUP BY source_file, reason
ORDER BY source_file, rows_rejected DESC, reason;
```

**ELT SQL** (`mart_*` tables)

```sql
SELECT source_file, failure_reason, rows_rejected
FROM mart_dq_rejections
WHERE run_id = (SELECT max_by(run_id, run_date) FROM mart_dq_rejections)
ORDER BY source_file, rows_rejected DESC, failure_reason;
```

| source_file | failure_reason | rows_rejected |
|---|---|---|
| customers.csv | CUS_CUSTOMER_ID_UNIQUE | 5 |
| orders.csv | ORD_AMOUNT_MATCHES_FORMULA | 18 |
| orders.csv | ORD_CUSTOMER_ID_NOT_NULL | 18 |
| orders.csv | ORD_ORDER_ID_UNIQUE | 12 |
| orders.csv | ORD_QUANTITY_POSITIVE | 10 |
| orders.csv | ORD_PRODUCT_EXISTS | 9 |
| orders.csv | ORD_DELIVERED_HAS_MINUTES | 8 |
| orders.csv | ORD_ORDER_TS_NOT_FUTURE | 7 |
| orders.csv | ORD_DELIVERY_MINUTES_MAX | 6 |
| orders.csv | ORD_ORDER_TS_VALID | 5 |
| orders.csv | ORD_STORE_EXISTS | 5 |
| orders.csv | ORD_DISCOUNT_RANGE | 3 |
| orders.csv | ORD_STATUS_VALID | 3 |
| orders.csv | ORD_PAYMENT_MODE_VALID | 2 |
| orders.csv | ORD_ORDER_TS_NOT_NULL | 1 |
| payments.csv | PAY_AMOUNT_POSITIVE | 10 |
| payments.csv | PAY_ORDER_EXISTS | 8 |
| payments.csv | PAY_TS_NOT_FUTURE | 7 |
| payments.csv | PAY_PAYMENT_ID_UNIQUE | 4 |
| payments.csv | PAY_MODE_VALID | 2 |
| products.csv | PRD_PRODUCT_ID_UNIQUE | 2 |


**ETL result vs ELT result: IDENTICAL** (21 rows compared). A row failing two rules is counted under each reason. Customer/product/payment files appear in the full result.


## 7. Part F — Observability experiments

**How they were run.** Every experiment was executed on **both DAGs, on a live Airflow 3.3.2 scheduler**, by `experiments/run_experiments.py` (trigger → wait → record). Inputs are *copies* of the data
(`experiments/make_experiment_data.py`) and the warehouse is a *copy* of the baseline built by experiments 1–3, so the original files and the real warehouse are never touched — there is nothing to "restore".
The raw record of every run is in `experiments/results/*.json`. The first round of runs (before the fixes below) is kept in `experiments/results_before_fixes/`.

**What the experiments changed in the design.** Each row is a real defect or gap that an experiment exposed; all are covered by regression tests in `tests/nammamart/test_scenarios.py`.

| # | Found in | Finding (observed) | Fix |
|---|---|---|---|
| F1 | Exp 4 / 6 | A failed night showed as a **green DAG run**: the all-done audit task is the only leaf and it succeeds, so Airflow reported the run `success` while a task had failed. | New last task `run_verdict` (the only leaf, `all_done`) raises if anything upstream failed → the **run** turns red. |
| F2 | Exp 5 / 7 | The ELT used "latest *loaded* batch wins": re-delivering `orders.csv` (an older snapshot) **silently undid the 10 corrections** from the increment — ELT mart revenue drifted **+₹4,193.70** (Exp 5) and +₹532.80 (Exp 7) away from the ETL. | Explicit precedence: an increment outranks the base snapshot, a later increment outranks an earlier one (`stg_orders_batch.sql`); mirrors the ETL's file order. |
| F3 | Exp 7 | A 50-row orders file made the ETL reject **2,172 of 2,228 payments** as "orphans" (its checks had no memory); the ELT rejected only the true 8. | ETL payment checks now also know the orders loaded or rejected in earlier runs (`etl/history.py`). |
| F4 | Exp 8b | A *self-consistent* 100× price drift on one store (price and amount both ×100) passed every Critical check; only a Warning fired, and **total revenue went from ₹12.1 lakh to ₹83.8 lakh**. | New Critical check `ORD_PRICE_FAR_FROM_CATALOGUE` (a price more than 3× above or below the catalogue MRP is not believable). |
| F5 | Exp 6 / 4 | A schema break would burn retries for nothing; a missing file deserves them. | `AirflowFailException` on schema-contract errors only (fail fast); retries kept for a missing file. |
| F6 | Exp 4 | A failed ELT run reported `rows_quarantined = 142` — yesterday's staging verdict, not this run's. | The ELT audit reports rejects only if staging really ran in that run. |

---

### Experiment 1 — Happy path

* **Expected:** all tasks green; the five extracts in parallel; per-file in / passed / quarantined visible.
* **Observed — ETL (18 tasks, 19 s):** the **5 extracts all started at +0 s** (0.9–1.2 s each); the three dimension validations then ran in parallel; `validate_orders` waited for them, `validate_payments` for orders; `log_dq_results` and `quarantine_failed_rows` ran in parallel; then transform → load → reconcile → report → audit → verdict.
  **ELT (28 tasks, 43 s):** 5 extracts at +0 s; 5 raw loads started together at +1 s but **queued behind DuckDB's single-writer lock** (1.2–8.0 s); the **8 marts all started at +17 s** and finished over the next 21 s (1.6 s → 20.9 s) — the lock serialises them, so the ELT's apparent parallelism is not a real speed-up on DuckDB.
  Rows per file (identical in both pipelines): orders **2,412 in / 2,308 passed / 104 quarantined**; payments 2,228 / 2,197 / 31; customers 405 / 400 / 5; products 152 / 150 / 2; stores 15 / 15 / 0 → **5,212 in, 142 quarantined**. The ELT also loaded 5,212 raw rows. Net revenue ₹11,34,962.80 in both.
* **Where I saw it:** Graph / Grid / Gantt views; the task-log line `[Validate:orders] rows=2412 passed=2308 quarantined=104`; `pipeline_runs`; `dq_results`.
* **What I would change:** the per-file in / passed / quarantined counts live only in task logs — put them in `pipeline_runs.notes`. On a real warehouse the ELT marts would run truly in parallel; on DuckDB, merge small marts into fewer tasks or accept the queue.

### Experiment 2 — Re-run (idempotency)

* **Expected:** fact counts unchanged; the ELT raw tables do *not* grow for identical files.
* **Observed — ETL:** `fact_inserted 0, fact_updated 0, fact_unchanged 2,308`; the fact is still 2,308 rows, revenue still ₹11,34,962.80. (A new set of quarantine files is written per run — by design, it is an audit log.)
  **ELT:** all five files `skipped_unchanged` (their SHA-256 is already in `raw_file_registry`): `rows_loaded = 0`, **`raw_orders` stays 2,412**; MERGE `0 inserted, 0 updated, 2,400 unchanged`; mart revenue unchanged.
  *Is "raw did not grow" correct?* **Yes** — nothing new was received, so nothing new is recorded. Had the file changed it would be appended as a new batch (Exp 3 and 5); re-loading identical content would only duplicate the audit trail.
* **Where:** `pipeline_runs.rows_loaded` (ELT 0), `raw_file_registry`, `pipeline_runs.notes` (`skipped_unchanged`).
* **What I would change:** the ETL's `rows_loaded` counts rows written by the *idempotent* MERGE (2,873 even when nothing changed); reporting *changed* rows would make a no-op re-run show 0, like the ELT. (Inserted / updated / unchanged are already in `notes`.)

### Experiment 3 — Incremental load

* **Expected:** 150 new orders appended; the 10 corrected orders change status in place; no duplicates; a second run is a no-op.
* **Observed — ETL:** `fact_inserted 150, fact_updated 10, fact_unchanged 2,298` → **2,458** rows; `etl_order_changes` received 10 rows (DELIVERED → RETURNED, `net_revenue` → 0).
  **ELT:** only the increment was loaded (`orders_2026-10-01_increment.csv: loaded`, the other five `skipped_unchanged`): `raw_orders` 2,412 → 2,572; MERGE `150 inserted, 10 updated, 2,390 unchanged`; `stg_orders` 2,550 rows (2,458 clean); `stg_order_changes` 10.
  Revenue ₹12,10,421.30 in both; `count(*) − count(distinct order_id) = 0` in both; e.g. `ORD000337` is one row, `RETURNED`. **Re-run (3b): 0 inserted / 0 updated, raw unchanged.** The reconciliation after the increment adds 150 `PAYMENT_FEED_PENDING` (the gateway file has no 1 October payments) and 10 `RETURNED_NOT_REFUNDED` (₹4,193.70).
* **Where:** `pipeline_runs.notes`, `etl_order_changes` / `stg_order_changes`, `raw_file_registry`.
* **What I would change:** nothing structural. The increment relies on the file-name convention `orders_<date>_increment.csv`; an `updated_at` column in the source would be the first thing to ask the client for.

### Experiment 4 — Missing source file (`products.csv` renamed)

* **Expected:** retries, then failure; independent branches finish; dependent tasks never run; nothing wrong is loaded; I am told.
* **Observed (both DAGs started together, as at 09:00):** `extract_products` raised `FileNotFoundError: Source file not found …/products.csv` and made **3 attempts — ETL at 17:12:19, 17:13:21 and 17:14:23; ELT at 17:12:23, 17:13:24 and 17:14:25** (≈ 62 s apart: `retries=2`, `retry_delay=60 s`); final state `failed`.
  **ETL:** 7 success (the 4 other extracts, `validate_customers`, `validate_stores`, the audit), 2 failed (`extract_products`, `run_verdict`), 9 `upstream_failed`. **ELT:** 12 success, 2 failed, 14 `upstream_failed`. The warehouse was untouched (fact 2,458, revenue ₹12,10,421.30).
  The audit task still ran (`all_done`) and wrote **`FAILED`** with the exact failed-task list; the failure callback fired on the final attempt (log line `[Slack] No SLACK_WEBHOOK_URL set. Printing alert to console instead` — with the Variable set it posts to Slack). The run took 131 s.
  **Before fix F1** the DAG run was shown as **`success`** (green) although a task had failed (`experiments/results_before_fixes/exp4_missing_file_before_verdict_fix.json`); after F1 the run is `failed` (red).
* **Where:** Grid view (`up_for_retry` → `failed` → `upstream_failed`), the task log `attempt=3`, `pipeline_runs.status / failed_tasks`.
* **What I would change:** three attempts in two minutes is short for a late upstream export — use exponential back-off up to ~09:20 (or a file sensor with a timeout), and page on the *first* failure, not the third.

### Experiment 5 — New bad data (5 rows appended to a copy of `orders.csv`)

* **Expected:** each row quarantined with its own reason; `dq_results` moves by exactly one per check; none reaches the finance tables.
* **Observed (identical in both pipelines):**

| Row | Defect | Reason recorded |
|---|---|---|
| ORD900001 | null customer | `ORD_CUSTOMER_ID_NOT_NULL` |
| ORD900002 | negative quantity | `ORD_QUANTITY_POSITIVE` |
| ORD900003 | unknown store `S99` | `ORD_STORE_EXISTS` |
| ORD900004 | status `SHIPPED` | `ORD_STATUS_VALID` |
| ORD900005 | 2027 timestamp | `ORD_ORDER_TS_NOT_FUTURE` |

  ETL: 5 new rows in `etl_quarantine` and in the quarantine CSV; ELT: 5 `FAIL` rows in `stg_orders`. `dq_results`: customer-null 18 → 19, quantity 10 → 11, store 5 → 6, status 3 → 4, future 7 → 8; `rows_quarantined` 142 → **147**. `etl_fact_orders` and `v_orders_clean` contain **0** of them; revenue unchanged.
  The ELT's `raw_orders` grew 2,572 → 4,989 because the changed `orders.csv` (2,417 rows) was appended as a new batch — the price of keeping every delivered version.
  **Before fix F2** the ELT mart revenue also rose by exactly **₹4,193.70** (to ₹12,14,615.00) while the ETL stayed at ₹12,10,421.30: the re-delivered base file had outranked the increment and un-returned the 10 orders. After F2 both are ₹12,10,421.30.
* **Where:** `etl_quarantine`, `stg_orders.failure_reason`, the quarantine file, `dq_results`, `pipeline_runs.rows_quarantined`.
* **What I would change:** compact old raw batches (partition by month, expire per the retention policy) so re-delivered full files do not grow raw without bound.

### Experiment 6 — Schema change (`order_amount` renamed `total`)

* **Expected:** a loud failure at the earliest task; nothing loaded.
* **Observed:** `extract_orders` **failed on attempt 1 — no retries** (fail-fast, F5): `Error: orders.csv: schema changed. Missing columns: ['order_amount']; unexpected columns: ['total']`. ETL: 2 failed (extract + verdict), 8 `upstream_failed`, run `failed` in **6.6 s**; ELT: 2 failed, 14 `upstream_failed`, `failed` in 15.2 s. Nothing was loaded (`rows_loaded 0`, counts unchanged): **it failed loudly; it did not load wrong data silently.**
  *Reasoning, not an observation:* without the explicit header contract a positional CSV load would accept an in-place rename without complaint, and a column reorder would mis-map values silently — which is exactly why the check exists.
* **Where:** the task log of `extract_orders` (the message above), Grid view, `pipeline_runs.failed_tasks`.
* **What I would change:** extend the contract to column order and basic types, and version it — a *deliberate* schema change should be a reviewed change to `EXPECTED_COLUMNS`, not an incident.

### Experiment 7 — Volume anomaly (orders file cut to 50 rows)

* **Expected:** nothing warns unless a volume check exists. *(Honest note: I had read the experiment list and added `BATCH_ORDERS_VOLUME` up front rather than discovering the gap.)*
* **Observed:** **both** pipelines record `BATCH_ORDERS_VOLUME = WARN` (50 rows vs 2,412 the run before: shortfall 2,362) in `dq_results`; the run itself stays green. Revenue is unchanged in both (₹12,10,421.30 — the load only touches the 50 orders in the file).
  **Before fix F3** the ETL treated the 50-row file as the whole truth and **rejected 2,172 of 2,228 payments** (`PAY_ORDER_EXISTS`; `rows_quarantined 2,182`), while the ELT, which remembers earlier orders, rejected only the 8 genuine orphans. After F3 the ETL also rejects 8 (`rows_quarantined 40`). A design difference remains by nature: the ETL judges *today's files* (209 current order rows checked), the ELT judges *the current state* (2,561).
  **Before fix F2** the ELT also drifted by +₹532.80 here (one of the 10 returned orders is among the 50 rows).
* **Where:** `dq_results` (`BATCH_ORDERS_VOLUME`, `rows_checked 50`, `rows_failed 2362`), `pipeline_runs.rows_extracted` (3,010 vs 5,372).
* **What I would change:** a > 90 % drop in the main file is a broken export, not a quiet day — make it **block** the load (fail before `load_warehouse`) instead of a Warning nobody is paged for.

### Experiment 8 — Distribution drift (`unit_price` × 100 for store S08; amount untouched)

* **Expected:** possibly not caught — nothing I wrote looks at distributions.
* **Observed:** it **was** caught, but by accident: `order_amount` no longer matched `qty × price × (1 − disc)`, so `ORD_AMOUNT_MATCHES_FORMULA` rejected all of S08's September orders (18 → 193 failures) and `ORD_PRICE_FAR_FROM_CATALOGUE` flagged the same 174. `BATCH_STORE_REJECT_RATE` went `WARN` (1 of 15 stores above 25 % rejected) and `rows_quarantined` rose 142 → **310**. **The metric that moved:** S08 net revenue fell from **₹78,980.50 to ₹6,569.90 (−92 %)** and total delivered revenue from ₹12,10,421.30 to **₹11,38,010.70 (−₹72,410.60, −6.0 %)**; `etl_fact_orders` lost 168 orders. The run stays green; only the Warning and the revenue drop reveal it.
* **Where:** `dq_results` (`BATCH_STORE_REJECT_RATE`), `mart_daily_store_kpis` / `etl_daily_zone_revenue` for S08, `pipeline_runs.rows_quarantined`.
* **What I would change:** a store losing 92 % of its revenue overnight should page someone — add a *metric* guard on the marts (revenue per store vs its trailing average), not only row-level rules.

### Experiment 8b — Self-consistent drift (`unit_price` **and** `order_amount` × 100) — run because Exp 8 only worked by luck

* **Observed before fix F4:** every Critical check passed. Only `ORD_PRICE_MATCHES_CATALOGUE` (a Warning, 174 rows) fired, and **S08 revenue became ₹72,47,629.90 and total revenue ₹83,79,070.70 — 6.9× the truth — loaded into the CFO's tables** (`experiments/results_before_fixes/exp8b_consistent_drift.json`).
* **After F4** (`ORD_PRICE_FAR_FROM_CATALOGUE` is Critical): the 174 rows are quarantined in both pipelines, `BATCH_STORE_REJECT_RATE` warns, revenue is **₹11,38,010.70** (identical to Exp 8) and S08 is ₹6,569.90.
* **Lesson:** a Warning is, by definition, data you chose to load. Plausibility checks against an independent reference (the catalogue price) belong at Critical when the harm of loading is large.

### Experiment 9 — Freshness (newest order ≈ 30 days old at run time)

* **Expected:** the timeliness check warns.
* **Observed:** I set `as_of = 2026-10-31 09:00` (newest valid order 2026-10-01 22:33, ≈ 29.4 days old). **`BATCH_ORDERS_FRESH = WARN` in both pipelines** (`rows_checked 1, rows_failed 1`); the run is green. A side effect worth knowing: the future-date check is relative to run time, so `ORD_ORDER_TS_NOT_FUTURE` fell 7 → 5 (the two orders dated 2026-10-21 are no longer "in the future" on 31 October) and the fact gained those 2 orders (2,458 → 2,460) in both pipelines.
  (In this demo the check warns on *every* run, legitimately: the data is static and already older than a day.)
* **Where:** `dq_results.BATCH_ORDERS_FRESH`.
* **What I would change:** a stale export at 09:00 should alert the owner of the export (route this Warning to Slack), and the dashboard should print a "data as of" stamp.

### Experiment 10 — Recovery (fix the cause, then **Clear** the failed task)

* **Expected:** only the failed task and everything downstream re-run.
* **Observed:** after restoring `products.csv` I cleared `extract_products` *with downstream*, through the same REST endpoint as the UI's Clear button. **ETL: 12 tasks re-ran** (`extract_products` + 11 downstream, including the audit and the verdict) and **6 were left untouched** (4 extracts, `validate_customers`, `validate_stores`). **ELT: 17 re-ran, 11 untouched** (4 extracts, 5 raw loads incl. the increment, `stg_customers`, `stg_stores`). Both runs went `failed` → **`success`**; `pipeline_runs` for the *same `run_id`* was overwritten `FAILED` → `SUCCESS` (5,372 / 3,023 / 142), so there is no duplicate row; the attempt history still shows the three failed tries.
* **Where:** Grid view (only the cleared column turns yellow), `task_instance_history`, `pipeline_runs`.
* **What I would change:** overwriting the audit row loses the story "failed at 09:00, fixed at 09:14" — add an append-only `pipeline_run_events` table.

---

### The five pillars of data observability — where this setup stands

| Pillar | What exists | Evidence | Rating |
|---|---|---|---|
| **Freshness** | `BATCH_ORDERS_FRESH` (newest valid order ≤ 1 day old), the schedule + `pipeline_runs.start_time / end_time`, Airflow run history | Exp 9 | Good — but it only warns; nobody is paged |
| **Volume** | `BATCH_ORDERS_VOLUME` vs the previous run, `rows_extracted / rows_loaded / rows_quarantined` per run, `BATCH_STORE_REJECT_RATE` | Exp 1, 5, 7, 8 | Good — a Warning, it does not block |
| **Schema** | header contract at extract; fail-fast, no retries; nothing is loaded | Exp 6 | **Best covered** — hard stop, readable error, zero bad data |
| **Distribution** | only row-level plausibility (formula, price vs catalogue, delivery ≤ 120 min) and a per-store reject rate | Exp 8, 8b | **Weakest** — Exp 8b slipped through until I added a rule; there is no statistical profile (mean, percentiles, per-store trend) of the numbers |
| **Lineage** | ELT: `_source_file / _source_row_num / _batch_id / _run_id` through every layer, raw kept; ETL: `source_file / source_row_num` on the fact, quarantine files, change tables | §5 Q3, Q8, Q9 | Strong in ELT, partial in ETL (rejected rows exist only in per-run files) |


## 8. Part G — Recommendation to Sameer (CTO)

**To:** Sameer, CTO · **From:** Sudarshan, FDE · **Re:** ETL or ELT for NammaMart?

**Recommendation: adopt ELT.** Load every source file raw, validate and transform in SQL, and let *only validated rows* into the tables Finance reads. Add one ETL-style step at the front door: mask or tokenise phone and email **before** they land in the raw layer.

I built both on the same five exports and ran them side by side through ten experiments. They give **the same numbers to the paisa** (₹11,34,962.80 for September; ₹12,10,421.30 after the 1 October file; 181 reconciliation mismatches; 142 rejected rows), enforced by automated tests, so the choice is about *how each behaves when things go wrong*, not about correctness.

| Criterion | What I observed | Edge |
|---|---|---|
| **Auditability** | Finance's question "show me a rejected order, the rule it broke, and prove it isn't in revenue" is one place in the ELT (raw row → `failure_reason` → absent from `v_orders_clean`). In the ETL the rejected row exists only in a per-run quarantine file, and what the extract read is stored nowhere. When 10 September orders were returned on 1 October, the ELT raw layer still holds both versions of each (and the history table says when it changed). | **ELT** |
| **Data-quality guarantee** | Both enforce the same 66 rules. The ETL's guarantee is *structural*: a bad row never enters the warehouse. The ELT's depends on every reader going through the marts: staging deliberately holds rejected rows. Exp 8b is the warning for both: a Warning-level gap loaded ₹83.8 lakh of fake revenue until I made the check Critical. | **ETL** by construction (the ELT needs grants, one clean view and tests) |
| **Ease of change** | A rule is one SQL file in the ELT, re-applied to *everything already received* by rebuilding from raw. In the ETL a rule is Python plus a redeploy, and rows it quarantined or never loaded can only be recovered from files someone kept. The ELT also survived changes the ETL's stateless view struggled with (Exp 7: the ETL rejected 2,172 valid payments until I gave it memory). | **ELT** |
| **Performance at 1,000× the data** | Measured on this machine (`experiments/results/benchmark_scale.json`): at 2,412 orders the ETL is faster (2.1 s vs 7.9 s); at **241,200 orders the ELT is already faster** (15.4 s vs 23.9 s); at **723,600** it is **2.7× faster** (26.2 s vs 72.0 s) and uses less memory (1.2 GB vs 1.7 GB). The ETL costs ≈ 0.10 s per 1,000 orders, the ELT ≈ 0.022 s. *Extrapolating that straight line to 1,000× (≈ 2.4 M orders): ETL ≈ 4 min and ≈ 5.5 GB of RAM (pandas holds whole files in memory); ELT ≈ 1 min and ≈ 3 GB, and DuckDB can spill to disk.* | **ELT** from ≈ 30× |
| **Cost** | Everything is open source and either pipeline's compute is minutes a day. The ELT stores all raw data for years (about half a megabyte a month at today's size, roughly half a gigabyte at 1,000× before compression: cheap). Its real cost is **people and process**: access control, encryption and retention for the raw layer. The ETL costs RAM (a worker sized for the biggest file) and re-engineering as files grow. | **Even**; ETL is cheaper to *govern* |

**Where the ETL is genuinely better.** At today's size it is simpler to explain and 4× faster; bad rows and raw personal data *never enter* the warehouse; it needs no raw-layer governance. If NammaMart's data stayed this small, this stable and always fully re-deliverable, I would keep the ETL.

### Top 3 risks of the recommended design
1. **Raw personal data in the warehouse.** `raw_customers` holds real phone numbers and emails, and a DuckDB file has no roles. *Mitigate:* tokenise at load (the front-door step above), put raw behind RBAC and encryption, run the retention job (§5 Q4–Q6), and get the DPO's sign-off before go-live.
2. **A bad or implausible row reaching revenue.** Staging contains rejected rows, and Warnings load by definition (Exp 8b). *Mitigate:* analysts get access to the marts only; every mart reads the single `v_orders_clean`; keep plausibility checks Critical; add a metric guard (revenue per store versus its trailing average) because Exp 8 showed a store losing 92 % of its revenue with a green run.
3. **A single-node, single-writer, laptop-hosted pipeline.** DuckDB serialises writers (the marts queued 21 s in Exp 1), a laptop that is off at 09:00 misses the SLA (§3), and a partial or re-delivered file can distort the state (Exp 5 and 7 produced two real bugs). *Mitigate:* run on an always-on scheduler, move the marts to a warehouse with concurrent writers when volume warrants it, block the load on a > 90 % volume drop, and ask the client for an `updated_at` column so "which version is newer" never depends on file names.

**What would change my mind:** if the legal answer on keeping raw customer data is "not even tokenised", or the exports stay small and fully reloadable for good, the ETL is the safer choice.


## 9. Bonus items, assumptions, limitations

### 9.1 Bonus items (all four are built and tested)

| Bonus | What | Where / evidence |
|---|---|---|
| **Slack alert on failure via an Airflow Variable** | `default_args["on_failure_callback"] = notify_failure`: reads `Variable.get("SLACK_WEBHOOK_URL")` at alert time and posts to Slack; with no Variable it logs the alert instead (verified firing in the final attempt's log in Exp 4: `[Slack] No SLACK_WEBHOOK_URL set. Printing alert to console instead`). I did not configure a real webhook, so no message was actually posted. | `include/nammamart/audit.py`; screenshot `10b_…` |
| **SCD Type 2 customer dimension** | `etl_dim_customer_scd2` (customer_id, loyalty_tier, valid_from, valid_to, is_current). A tier change closes the open row and opens a new one; an unchanged tier adds nothing (idempotent). Test: 3 customers promoted → 403 rows, 400 current, then a re-run adds none. | `include/sql/etl_tables.sql`, `etl_load_dims.sql`; `test_scd2_keeps_loyalty_tier_history` |
| **Store-capacity check** | `mart_store_capacity` (ELT) and `etl_store_capacity` (ETL): orders placed per store per day against `capacity_orders_per_day`, with `utilisation_pct` and `over_capacity`. Result on this data: **no store-day is over capacity** (peak 6.0 %, store S11 on 1 Oct: 18 orders vs 300); S12's capacity is 0 in the master, so its 31 store-days show NULL utilisation rather than a divide-by-zero. | `include/sql/mart_store_capacity.sql`; `test_store_capacity_tables_agree_…` |
| **Data-quality trend chart from `dq_results`** | A self-contained HTML/SVG chart of rows failed per run, stacked by dimension, with the % of checks that passed above each bar (36 runs: the four demo runs plus every experiment). | `submission/dq_trend_chart.py` → `submission/dq_trend_etl.html`, `dq_trend_elt.html`; screenshot `16_…` |

### 9.2 Assumptions I made (each is a question for the client)

* Source timestamps are **IST**; `as_of` ("now" for the future-date and freshness checks) is the run's IST time unless overridden.
* `customers`, `products`, `stores` and `payments.csv` are **full snapshots**; the newest delivery is the truth.
* `orders.csv` is the base snapshot and `orders_<date>_increment.csv` files are applied on top (later date wins); an increment always outranks the base.
* `upi`, `UPI `, `CARD`, `cod` are known aliases of valid payment modes; `Cashh` is **not** guessed — it is rejected.
* A DELIVERED order whose payment FAILED is still revenue (the order was delivered); it is listed in the reconciliation as `PAYMENT_FAILED` for Finance to chase.
* Name suffix `sudarshan` is used for DAG ids, the warehouse file and the quarantine folder (a single constant, `OWNER` in `include/nammamart/config.py`, plus the two DAG file names).

### 9.3 Known limitations (honest list)

* **Docker / Astro were not available on this machine.** All runs, tests and screenshots are from a real Apache Airflow 3.3.2 (api-server + scheduler + DAG processor) in WSL, not from `astro dev start`. Retake the screenshots in the Astro UI if the Docker look is required.
* **DuckDB has no roles and one writer.** Raw PII is only protected by process here; a production design needs RBAC/encryption (see Part G risk 1). The ELT marts queue on the writer lock.
* **Snapshot semantics.** Records that disappear from a newer snapshot are not deleted downstream; the ETL judges *today's files* while the ELT judges *the current state* (visible in Exp 7 as different `rows_checked`, but with identical revenue).
* `pipeline_runs` is overwritten when a run is cleared and re-run (Exp 10); an append-only events table is the proposed fix.
* The ETL's `rows_loaded` counts rows written by the idempotent MERGE (fact + dimensions), the ELT's counts rows newly appended to raw: the columns are comparable in intent, not in definition.
* The 1,000× figures in Part G are a **linear extrapolation** of measurements at 1×–300×, not a measurement.

