"""
Shared fixtures: run the ETL and the ELT once each on the real NammaMart files (into throw-away folders, never the
real warehouse) and let the tests compare what they produced.
"""

from __future__ import annotations

import contextlib
import io

import duckdb
import pytest

from nammamart.config import ELT_DAG_ID, ETL_DAG_ID, resolve_runtime
from nammamart.elt.tasks import run_elt_locally
from nammamart.etl.tasks import run_etl_locally

AS_OF = "2026-10-06 09:00:00"


def _runtime(root, name: str, run_id: str, increments: bool, warehouse: str = "wh.duckdb", data_dir: str | None = None,
             as_of: str = AS_OF):
    return resolve_runtime(
        {
            "data_dir": data_dir,
            "warehouse_path": str(root / name / warehouse),
            "quarantine_dir": str(root / name / "quarantine"),
            "work_dir": str(root / name / "work"),
            "as_of": as_of,
            "load_increments": increments,
        },
        run_id,
    )


def _quiet(fn, *args, **kwargs):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*args, **kwargs)


class Pipelines:
    """Runs both pipelines and exposes read-only connections to their warehouses."""

    def __init__(self, root):
        self.root = root
        self.results: dict[str, dict] = {}

    def run(self, label: str, run_id: str, increments: bool, data_dir: str | None = None, as_of: str = AS_OF):
        etl = _quiet(run_etl_locally, _runtime(self.root, "etl", f"etl_{run_id}", increments, data_dir=data_dir, as_of=as_of), ETL_DAG_ID)
        elt = _quiet(run_elt_locally, _runtime(self.root, "elt", f"elt_{run_id}", increments, data_dir=data_dir, as_of=as_of), ELT_DAG_ID)
        self.results[label] = {"etl": etl, "elt": elt}
        return self.results[label]

    def etl(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.root / "etl" / "wh.duckdb"), read_only=True)

    def elt(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.root / "elt" / "wh.duckdb"), read_only=True)


@pytest.fixture(scope="module")
def base_run(tmp_path_factory):
    """Both pipelines after ONE run on the September files only (no increment)."""
    p = Pipelines(tmp_path_factory.mktemp("base"))
    p.run("run1", "run1", increments=False)
    return p


@pytest.fixture(scope="module")
def full_run(tmp_path_factory):
    """Both pipelines: base run, re-run, then a run that also picks up the 2026-10-01 increment, then a re-run of that."""
    p = Pipelines(tmp_path_factory.mktemp("full"))
    p.run("run1", "run1", increments=False)
    p.run("run2_rerun", "run2", increments=False)
    p.run("run3_increment", "run3", increments=True)
    p.run("run4_rerun_increment", "run4", increments=True)
    return p


@pytest.fixture(scope="session")
def experiment_data(tmp_path_factory):
    """The Part F experiment inputs (copies of the real files with deliberate damage)."""
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "experiments"))
    import make_experiment_data

    return make_experiment_data.build(tmp_path_factory.mktemp("experiment_data"))
