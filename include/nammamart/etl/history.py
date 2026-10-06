"""
include/nammamart/etl/history.py
--------------------------------
What the ETL warehouse already knows about orders from EARLIER runs.

Why: validation judges today's files, but a payment may legitimately belong to an order that is not in today's orders
file (a partial or late export). Without history, such a day turns ~every payment into an "orphan" and floods quarantine
(seen in Experiment 7: 2,172 of 2,228 payments rejected because the orders file had only 50 rows). With history, an
order counts as known if it was loaded into the fact table, or rejected into quarantine, in any earlier run - the same
memory the ELT gets for free from stg_orders.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from nammamart.warehouse import warehouse


def known_orders(rt: dict) -> tuple[set[str], pd.Series]:
    """(order ids ever loaded or rejected, order_amount of the loaded ones). Empty on the very first run."""
    path = Path(rt["warehouse"])
    if not path.exists():
        return set(), pd.Series(dtype=float)
    with warehouse(path) as con:
        try:
            loaded = con.execute("SELECT order_id, CAST(order_amount AS DOUBLE) FROM etl_fact_orders").fetchall()
            rejected = con.execute("SELECT DISTINCT row_key FROM etl_quarantine WHERE source_file LIKE 'orders%'").fetchall()
        except duckdb.CatalogException:          # tables not created yet
            return set(), pd.Series(dtype=float)
    amounts = pd.Series({oid: amount for oid, amount in loaded}, dtype=float)
    return {oid for oid, _ in loaded} | {key for (key,) in rejected}, amounts
