# -*- coding: utf-8 -*-

import pytest

from spectron import parse_date


@pytest.mark.parametrize("s, expected", [("", 0), ("a", 0), ("1", 1), ("a1b2", 2),])
def test_num_digits(s, expected):
    assert parse_date.num_digits(s) == expected


@pytest.mark.parametrize(
    "s, expected",
    [
        ("test1digit", None),
        ("2020", None),
        ("12345678", None),
        ("2020-05-01", "DATE"),
        ("20200501", "DATE"),
        ("2020-05-01 00:00:00", "TIMESTAMP"),
        ("2020-05-01T12:34:56", "TIMESTAMP"),
        ("2020-05-01T12:34:56.123", "TIMESTAMP"),
        ("2020-05-01 12:34:56.123+0000", "TIMESTAMP"),
        ("2020-05-01T12:34:56.123+0000", "TIMESTAMP"),
        ("2020-05-01T12:34:56+0000", "TIMESTAMP"),
        ("2020-05-01T12:34:56.123+8000", "TIMESTAMP"),
    ],
)
def test_guess_type(s, expected):
    assert parse_date.guess_type(s) == expected
