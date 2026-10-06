"""
include/nammamart/standardise.py
--------------------------------
Vectorised parsing / standardising helpers for the ETL (pandas). Every parser is STRICT on purpose:
the ELT SQL uses the same regex + `try_strptime` / `TRY_CAST` rules so both pipelines accept and reject
exactly the same values.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from nammamart.config import PAYMENT_MODE_ALIASES

TS_FORMAT = "%Y-%m-%d %H:%M:%S"
TS_REGEX = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
DATE_REGEX = r"^\d{4}-\d{2}-\d{2}$"
NUM_REGEX = r"^[+-]?\d+(\.\d+)?$"
INT_REGEX = r"^[+-]?\d+$"
EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


def strip(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def parse_ts(s: pd.Series) -> pd.Series:
    """Strict 'YYYY-MM-DD HH:MM:SS'. Anything else (N/A, 31/09/2026 10:00, 2026-13-05 ...) -> NaT."""
    s = strip(s)
    ok = s.str.match(TS_REGEX)
    parsed = pd.to_datetime(s.where(ok), format=TS_FORMAT, errors="coerce")
    return parsed


def parse_date(s: pd.Series) -> pd.Series:
    s = strip(s)
    ok = s.str.match(DATE_REGEX)
    return pd.to_datetime(s.where(ok), format="%Y-%m-%d", errors="coerce")


def parse_num(s: pd.Series, integer: bool = False) -> pd.Series:
    """Numeric text -> float; anything non-numeric (blank, N/A, 12abc) -> NaN."""
    s = strip(s)
    ok = s.str.match(INT_REGEX if integer else NUM_REGEX)
    return pd.to_numeric(s.where(ok), errors="coerce")


def std_payment_mode(s: pd.Series) -> pd.Series:
    """'upi', 'UPI ', 'CARD', 'cod' -> canonical value; unknown spellings (e.g. 'Cashh') -> NaN (rejected, not guessed)."""
    return strip(s).str.lower().map(PAYMENT_MODE_ALIASES)


def capitalize(s: pd.Series) -> pd.Series:
    """'bronze' / 'BRONZE' -> 'Bronze' (first letter upper, rest lower)."""
    s = strip(s)
    return s.str[:1].str.upper() + s.str[1:].str.lower()


def join_reasons(masks: dict[str, pd.Series], order: list[str], index) -> pd.Series:
    """Per row, '; '-join the names of the checks whose mask is True, in catalogue order ('' when none)."""
    names = [n for n in order if n in masks]
    if not names:
        return pd.Series("", index=index, dtype=object)
    grid = pd.DataFrame({n: masks[n].astype(bool) for n in names}, index=index).to_numpy()
    labels = np.array(names, dtype=object)
    return pd.Series(["; ".join(labels[row]) for row in grid], index=index, dtype=object)
