# -*- coding: utf-8 -*-

import pytest

from spectron.data_types import set_dtype


@pytest.mark.parametrize(
    "val, expected",
    [
        ("str", "VARCHAR"),
        (1.234, "FLOAT4"),
        (281474976710655.0, "FLOAT8"),
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


@pytest.mark.parametrize("val", [float(2 ** (64 - 15) // 2), (2 ** 64 // 2)])
def test__set_dtype__out_of_bounds(val):
    with pytest.raises(OverflowError):
        set_dtype(val, strict=True)
