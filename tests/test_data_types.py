# -*- coding: utf-8 -*-

import pytest

from spectron.data_types import set_dtype


@pytest.mark.parametrize(
    "val, expected",
    [
        ("str", "VARCHAR"),
        (1.234, "FLOAT4"),
        (281474976710656.1, "FLOAT8"),
        (1, "SMALLINT"),
        (32767, "SMALLINT"),
        (32768, "INT"),
        (2147483647, "INT"),
        (2147483648, "BIGINT"),
        (9223372036854775807, "BIGINT"),
        (True, "BOOL"),
    ],
)
def test__set_dtype(val, expected):
    assert set_dtype(val) == expected


@pytest.mark.xfail(raises=ValueError)
def test__set_dtype__out_of_bounds():
    set_dtype(2 ** 64 // 2)
