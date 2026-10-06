# `submission/` — evidence index

The write-up is **[`../SUBMISSION.md`](../SUBMISSION.md)**. This folder holds the evidence it refers to.

| Item | What it is | Produced by |
|---|---|---|
| `data_issues_log.csv` | Part 0 — **226 row-level data issues** (file, row key, source line, column, problem, dimension, proposed rule, severity, raw value; PII masked) | `python include/profiling/build_issues_log.py` |
| `data_issues_summary.csv` | the same log grouped by rule | same |
| `queries/business_questions_etl.sql`, `…_elt.sql` | the six business questions, one simple query each, for each pipeline | hand-written |
| `run_business_questions.py` | runs those queries and prints Markdown | — |
| `results/business_questions_etl_results.md`, `…_elt_results.md` | the answers (SQL + result), identical from both pipelines | `run_business_questions.py` |
| `results/lineage_audit_change.md` | Part E 3 / 8 / 9: lineage of one mart number, audit of a rejected order, change history of a returned order | `python submission/trace_examples.py` |
| `results/pii_masking_check.md` | Criterion 8 proof: no real customer email / phone / name in any non-raw table (29 tables, every text column scanned); exit code 1 if a leak is ever found | `python submission/verify_pii_masking.py --warehouse …` |
| `evidence/quarantine_sample/` | one set of real quarantine files from the scheduled run (`<file>_failed_<run_id>.csv`, with `failure_reason`; customer PII masked) | ETL DAG, task `quarantine_failed_rows` |
| `evidence/query_results/` | `dq_results`, `pipeline_runs` and `etl_payment_reconciliation` / `mart_payment_reconciliation` as Markdown + HTML | `evidence/build_query_results.py` |
| `screenshots/` | Airflow UI: both DAGs in Graph view (green), the 09:00 IST schedule with next-run time, a red failed run, task parallelism | captured from Airflow 3.3.2 running these DAGs |
| `dq_trend_chart.py`, `dq_trend_*.html` | bonus: data-quality trend chart built from `dq_results` | `python submission/dq_trend_chart.py …` |
| `../experiments/results/*.json` | Part F — raw record of every experiment on both DAGs (task states, retry attempts, `pipeline_runs`, `dq_results`, counts) | `python experiments/run_experiments.py` |
| `../experiments/results_before_fixes/` | the first round of Part F results, **before** the fixes the experiments led to | same (earlier code) |
| `../experiments/results/benchmark_scale.json` | Part G — measured scaling of ETL vs ELT | `python experiments/benchmark_scale.py` |

Nothing in this folder contains an unmasked email, phone number or customer name, a key, a password or a webhook URL.
