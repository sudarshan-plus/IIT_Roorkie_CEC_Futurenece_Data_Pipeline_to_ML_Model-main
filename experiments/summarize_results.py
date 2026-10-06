"""
experiments/summarize_results.py
--------------------------------
Prints a compact Markdown digest of experiments/results/*.json (what run_experiments.py recorded).

    python experiments/summarize_results.py              # everything
    python experiments/summarize_results.py exp5 exp7    # file-name prefixes
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"
SHORT = {"nammamart_etl_sudarshan": "ETL", "nammamart_elt_sudarshan": "ELT"}


def load(prefixes: list[str]) -> list[tuple[str, dict]]:
    out = []
    for path in sorted(RESULTS.glob("*.json")):
        if not prefixes or any(path.stem.startswith(p) for p in prefixes):
            out.append((path.stem, json.loads(path.read_text())))
    return out


def tasks_line(rep: dict) -> str:
    return ", ".join(f"{v} {k}" for k, v in rep["states"].items())


def parallel_line(rep: dict) -> str:
    starts: dict[float, list[str]] = {}
    for t in rep["tasks"]:
        if t["start_offset_s"] is not None:
            starts.setdefault(round(t["start_offset_s"]), []).append(t["task_id"])
    groups = [f"+{k}s: {', '.join(sorted(v))}" for k, v in sorted(starts.items()) if len(v) > 1]
    return "; ".join(groups[:4])


def main() -> None:
    for name, data in load(sys.argv[1:]):
        print(f"\n### {name}")
        for dag, rep in data.items():
            if not isinstance(rep, dict) or "tasks" not in rep:
                continue
            tag = SHORT.get(dag, dag)
            audit = rep.get("pipeline_runs") or {}
            print(f"- **{tag}**: run `{rep['final_state']}` in {rep['run_duration_s']}s; tasks: {tasks_line(rep)}")
            if audit:
                print(f"  - pipeline_runs: status={audit.get('status')} extracted={audit.get('rows_extracted')} "
                      f"loaded={audit.get('rows_loaded')} quarantined={audit.get('rows_quarantined')} "
                      f"failed_tasks=[{audit.get('failed_tasks')}]")
            counts = rep.get("counts", {})
            keep = {k: v for k, v in counts.items() if v not in (None, 0)}
            if keep:
                print(f"  - counts: {keep}")
            if rep.get("retry_attempts"):
                att = "; ".join(f"{a['task_id']} try{a['try_number']} {a['state']} {a['start']}->{a['end']}"
                                for a in rep["retry_attempts"])
                print(f"  - retry attempts: {att}")
            fails = rep.get("dq_checks_with_failures", [])
            interesting = [f for f in fails if f["check_name"].startswith("BATCH_") or "ORD9" in json.dumps(rep.get("detail", {}))]
            if interesting:
                print("  - batch checks: " + "; ".join(f"{f['check_name']} {f['status']} {f['rows_failed']}/{f['rows_checked']}" for f in interesting))
            if "reran_after_clear" in rep:
                print(f"  - re-ran after Clear: {rep['reran_after_clear']}")
                print(f"  - NOT re-run: {rep['not_rerun']}")
        if "counts_before" in data:
            print(f"- counts before: { {k: v for k, v in data['counts_before'].items() if v} }")
        if "detail" in data:
            print(f"- detail: {json.dumps(data['detail'], default=str)}")
        elif any(isinstance(v, dict) and "detail" in v for v in data.values()):
            pass


if __name__ == "__main__":
    main()
