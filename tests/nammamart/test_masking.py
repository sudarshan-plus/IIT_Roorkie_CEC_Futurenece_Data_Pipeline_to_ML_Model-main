"""Masking rules. The examples are synthetic on purpose: no real customer value from the dataset appears outside include/data."""

import pytest

from nammamart.masking import mask_email, mask_name, mask_phone


@pytest.mark.parametrize("raw,expected", [
    ("jane.doe123@example.com", "ja***@example.com"),
    ("  JANE@Example.com ", "JA***@Example.com"),
    ("a@example.com", "a***@example.com"),
    ("sam.lee45example.com", "sa***"),              # malformed but present: still masked
    ("N/A", None), ("", None), (None, None),
])
def test_mask_email(raw, expected):
    assert mask_email(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("9876543210", "98******10"),
    ("991234567", "99*****67"),                      # 9 digits (invalid) is still somebody's number
    ("9801234", "98***34"),
    ("1234", "****"),
    ("N/A", None), ("", None),
])
def test_mask_phone(raw, expected):
    assert mask_phone(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("Jane Doe", "J. D."), ("  sam   lee ", "S. L."), ("Madonna", "M."), ("", None),
])
def test_mask_name(raw, expected):
    assert mask_name(raw) == expected


def test_no_raw_value_survives_masking():
    for fn, raw, fragments in (
        (mask_email, "jane.doe123@example.com", ("doe123", "jane.doe")),
        (mask_phone, "9876543210", ("76543", "987654")),
        (mask_name, "Jane Doe", ("Jane", "Doe")),
    ):
        masked = fn(raw)
        assert raw not in masked and not any(f in masked for f in fragments)
