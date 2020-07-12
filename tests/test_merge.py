# -*- coding: utf-8 -*-

import pytest

from spectron import merge


_dicts = [
    ({"a": None}),
    ({"a": {}}),
    ({"a-0": None, "a-1": None}),  # 2 top level keys
    ({"a": {"b": None}}),  # 1 nested key
    ({"array_parent": []}),  # 1 array
    ({"s": {"array_parent": []}}),  # 1 nested array
    ({"a": {"array-parent": [{"array-child-dict": None}]}}),  # nested dict in array
    ({"a": {"b": [{"c": None}], "b2": {"d": None}}}),  # 2 nested dicts
    ({"a": {"b": [{"c": [{"d": []}]}]}}),  # dicts.array.dict.array
]


@pytest.mark.parametrize(
    "d",
    [
        ({"a": {"b": None}}),
        ({"a": {"array-parent": [{"array-child-dict": None}]}}),
        ({"a": {"b": {"c": {"array-parent": [[{"nested-array-child-dict": None}]]}}}}),
    ],
)
def test__construct_single_branch(d):
    """Test single linear branch."""

    group_key = list(merge.extract_terminal_keys(d))[0][0]

    xd, xloc = {}, {}
    xd, xloc = merge.construct_branch(xd, xloc, group_key)
    assert d == xd


@pytest.mark.parametrize(
    "d",
    [
        ({"a": {"b": None}}),
        ({"a": {"array-parent": [{"array-child-dict": None}]}}),
        ({"a": {"b": {"c": {"array-parent": [[{"nested-array-child-dict": None}]]}}}}),
        *_dicts,
    ],
)
def test__construct_multi_branch(d):
    """Test construct branches."""

    xd, xloc = {}, {}
    for gk, val in merge.extract_terminal_keys(d):
        is_dict = isinstance(val, dict)
        merge.construct_branch(xd, xloc, gk, is_dict=is_dict)

    assert d == xd


@pytest.mark.parametrize("d", _dicts)
def test__construct_branch_idempotent(d):
    """Confirm idempotent."""

    key_store = merge.extract_terminal_keys(d)

    xd, xloc = {}, {}
    for gk, val in key_store:
        is_dict = isinstance(val, dict)
        merge.construct_branch(xd, xloc, gk, is_dict=is_dict)

    reconstructed_dict = xd.copy()

    for gk, val in key_store:
        is_dict = isinstance(val, dict)
        merge.construct_branch(xd, xloc, gk, is_dict=is_dict)

    assert reconstructed_dict == xd
