# -*- coding: utf-8 -*-

import pytest
from spectron.MaxDict import MaxDict


d_no_array_3 = {"a": {"b": {"c": 1}}}
d_no_array_4 = {"a": {"b": {"c": {"d": 1}}}}
d_array_nested = {"a": {"b": [{"c": 1}]}}
d_terminal_array = {"a": {"b": {"c": [1]}}}


@pytest.mark.parametrize(
    "vals, expected",
    [
        # test terminal override
        ([d_no_array_3, d_no_array_4], {"a": {"b": {"c": {"d": 1}}}},),
        # test array override
        ([d_no_array_3, d_array_nested], {"a": {"b": [{"c": 1}]}},),
        # test terminal override with array key
        ([d_no_array_3, d_terminal_array], {"a": {"b": {"c": [1]}}},),
        # test non-array override
        ([d_no_array_3, d_no_array_3, d_array_nested], {"a": {"b": {"c": 1}}},),
    ],
)
def test__overrides(vals, expected):

    md = MaxDict()
    md.batch_load_dicts(vals)
    assert md.asdict() == expected


def test__add_max_dicts():
    d1 = {
        "a1": {"b1": 1, "b2": "test", "b3": "x"},
        "a2": -100,
        "a3": {"b3": [{"c3": False}]},
        "a4": 1.234,
    }
    d2 = {
        "a1": {"b1": 10 ** 10, "b2": "x", "b3": "test"},
        "a2": 1,
        "a3": {"b3": [{"c3": True}]},
        "a4": 2.345,
    }

    md1 = MaxDict()
    md1.load_dict(d1)
    md2 = MaxDict()
    md2.load_dict(d2)

    mdx = md1 + md2

    expected = {
        "a1": {"b1": 10000000000, "b2": "test", "b3": "test"},
        "a2": -100,
        "a3": {"b3": [{"c3": True}]},
        "a4": 2.345,
    }

    assert mdx.asdict() == expected
