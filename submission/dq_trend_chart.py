"""
submission/dq_trend_chart.py
----------------------------
BONUS - data-quality trend chart built from `dq_results`.

Reads dq_results from one or more warehouse files (one run per x position, oldest first) and writes a self-contained
HTML page with an inline SVG: stacked bars of rows failed per run, split by quality dimension, plus the share of checks
that passed. No plotting library needed.

    python submission/dq_trend_chart.py --dag nammamart_etl_sudarshan --out submission/dq_trend_etl.html  wh1.duckdb [wh2.duckdb ...]
"""

from __future__ import annotations

import argparse
import html
from pathlib import Path

import duckdb

DIMENSIONS = ["Completeness", "Uniqueness", "Validity", "Accuracy", "Consistency", "Timeliness"]
COLORS = {"Completeness": "#4e79a7", "Uniqueness": "#f28e2b", "Validity": "#59a14f",
          "Accuracy": "#e15759", "Consistency": "#b07aa1", "Timeliness": "#76b7b2"}


def load_runs(paths: list[str], dag_id: str) -> list[dict]:
    runs: dict[tuple, dict] = {}
    for path in paths:
        con = duckdb.connect(path, read_only=True)
        rows = con.execute(
            "SELECT run_id, dimension, severity, rows_failed, status, min(checked_at) OVER (PARTITION BY run_id) "
            "FROM dq_results WHERE dag_id = ?", [dag_id]).fetchall()
        con.close()
        for run_id, dim, sev, failed, status, ts in rows:
            run = runs.setdefault((path, run_id), {"run_id": run_id, "ts": ts, "failed": {d: 0 for d in DIMENSIONS},
                                                   "checks": 0, "passed": 0, "critical_fail_checks": 0})
            run["failed"][dim] += failed
            run["checks"] += 1
            run["passed"] += status == "PASS"
            run["critical_fail_checks"] += status == "FAIL"
    return sorted(runs.values(), key=lambda r: r["ts"])


def svg(runs: list[dict], title: str) -> str:
    width, height, left, bottom, top = 1000, 460, 70, 120, 50
    plot_w, plot_h = width - left - 30, height - top - bottom
    peak = max((sum(r["failed"].values()) for r in runs), default=1) or 1
    scale = plot_h / (peak * 1.1)
    bar_w = max(8, min(46, plot_w / max(len(runs), 1) * 0.6))
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
           f'<text x="{left}" y="28" font-size="18" font-weight="600" fill="currentColor">{html.escape(title)}</text>']
    for i in range(0, 6):                                    # y grid
        value = peak * 1.1 * i / 5
        y = top + plot_h - value * scale
        out.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width - 20}" y2="{y:.1f}" stroke="currentColor" stroke-opacity=".15"/>')
        out.append(f'<text x="{left - 8}" y="{y + 4:.1f}" font-size="11" text-anchor="end" fill="currentColor">{value:.0f}</text>')
    step = plot_w / max(len(runs), 1)
    for i, run in enumerate(runs):
        x = left + step * i + (step - bar_w) / 2
        y = top + plot_h
        for dim in DIMENSIONS:
            h = run["failed"][dim] * scale
            if h > 0:
                y -= h
                out.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}" fill="{COLORS[dim]}">'
                           f'<title>{dim}: {run["failed"][dim]} rows failed</title></rect>')
        parts = run["run_id"].split("_")
        label = "_".join(parts[:2]) if parts[0] == "demo" else parts[0].replace("scheduled", "sched")   # exp5_etl_… -> exp5
        out.append(f'<text x="{x + bar_w / 2:.1f}" y="{top + plot_h + 14}" font-size="10" text-anchor="end" fill="currentColor" '
                   f'transform="rotate(-40 {x + bar_w / 2:.1f} {top + plot_h + 14})">{html.escape(label)}</text>')
        pct = 100.0 * run["passed"] / max(run["checks"], 1)
        out.append(f'<text x="{x + bar_w / 2:.1f}" y="{top + plot_h - sum(run["failed"].values()) * scale - 6:.1f}" font-size="10" '
                   f'text-anchor="middle" fill="currentColor">{pct:.0f}%</text>')
    out.append(f'<text x="{left}" y="{height - 8}" font-size="11" fill="currentColor">Bars: rows failed per run by dimension. '
               f'Label above a bar: % of the {runs[0]["checks"] if runs else 0} checks that passed.</text>')
    lx = left
    for dim in DIMENSIONS:                                   # legend
        out.append(f'<rect x="{lx}" y="38" width="12" height="12" fill="{COLORS[dim]}"/>'
                   f'<text x="{lx + 16}" y="48" font-size="12" fill="currentColor">{dim}</text>')
        lx += 120
    out.append("</svg>")
    return "\n".join(out)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("warehouses", nargs="+")
    parser.add_argument("--dag", default="nammamart_etl_sudarshan")
    parser.add_argument("--out", default="submission/dq_trend.html")
    args = parser.parse_args()
    runs = load_runs(args.warehouses, args.dag)
    title = f"Data-quality trend - rows failed per run ({args.dag})"
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>DQ trend</title><style>:root{{color-scheme:light dark}}body{{font-family:system-ui,sans-serif;margin:16px auto;max-width:1040px;padding:0 16px}}
svg{{width:100%;height:auto}}p{{opacity:.75}}</style></head><body>{svg(runs, title)}
<p>{len(runs)} runs read from dq_results. A step change between bars is the signal worth investigating.</p></body></html>"""
    Path(args.out).write_text(page, encoding="utf-8")
    print(f"{len(runs)} runs -> {args.out}")


if __name__ == "__main__":
    main()
