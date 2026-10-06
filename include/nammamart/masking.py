"""
include/nammamart/masking.py
----------------------------
PII masking used by the ETL transform (the ELT staging SQL implements the identical rules, and
tests/nammamart/test_parity.py proves the two outputs are the same).

  email  jane.doe123@example.com    -> ja***@example.com   (first 2 chars of the name part + domain)
  phone  9876543210                 -> 98******10          (first 2 + last 2 digits)
  name   Jane Doe                   -> J. D.               (initials)

Values that are blank or "N/A" have nothing to protect and become NULL. A value that is present but
malformed (e.g. a 9 digit phone) is still masked - it is still somebody's data.
"""

from __future__ import annotations

import pandas as pd

_EMPTY = {"", "N/A"}


def mask_email(value) -> str | None:
    v = _clean(value)
    if v is None:
        return None
    if "@" in v:
        local, _, domain = v.partition("@")
        return f"{local[:2]}***@{domain}"
    return f"{v[:2]}***"


def mask_phone(value) -> str | None:
    v = _clean(value)
    if v is None:
        return None
    if len(v) < 5:
        return "*" * len(v)
    return f"{v[:2]}{'*' * (len(v) - 4)}{v[-2:]}"


def mask_name(value) -> str | None:
    v = _clean(value)
    if v is None:
        return None
    return " ".join(f"{token[0].upper()}." for token in v.split())


def _clean(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    v = str(value).strip()
    return None if v in _EMPTY else v
