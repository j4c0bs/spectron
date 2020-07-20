# -*- coding: utf-8 -*-

import pytest

from spectron.MaxDict import Field


none__vals = [None, None]
none__expected = [None, None]

int_none__vals = [None, 1, None]
int_none__expected = ["int", 1]

int_abs__vals = [1, 100, -100]
int_abs__expected = ["int", -100]

int_to_float__vals = [1, 0.1]
int_to_float__expected = ["int", 1]

float_to_int__vals = [0.1, 1]
float_to_int__expected = ["float", 0.1]

bool__vals = [True, False]
bool__expected = ["bool", False]

str__vals = ["x", "", "xx", ""]
str__expected = ["str", "xx"]

int_to_str__vals = [1, 2.1, "x", 1, "xx", 100, True]
int_to_str__expected = ["int", 100]


@pytest.mark.parametrize(
    "vals, expected",
    [
        (none__vals, none__expected),
        (int_none__vals, int_none__expected),
        (int_abs__vals, int_abs__expected),
        (int_to_float__vals, int_to_float__expected),
        (float_to_int__vals, float_to_int__expected),
        (bool__vals, bool__expected),
        (str__vals, str__expected),
        (int_to_str__vals, int_to_str__expected),
    ],
)
def test__Field(vals, expected):
    field = Field("test", vals[0])

    for v in vals[1:]:
        field.add(v)

    dtype, max_value = expected
    assert field.dtype == dtype
    assert field.max_value == max_value
