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
