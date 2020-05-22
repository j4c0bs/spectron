# -*- coding: utf-8 -*-

import json
import pytest

from spectron import ddl


def load_sql(path):
    return path.read_text().strip()


def load_json(path):
    return json.loads(path.read_text())


@pytest.mark.parametrize("filename", ["test_1_line", "nested_reserved"])
def test__from_dict__defaults(filename, datadir):

    input_path = datadir / f"{filename}.json"
    result_path = datadir / f"{filename}.sql"

    d = load_json(input_path)
    assert ddl.from_dict(d).strip() == result_path.read_text().strip()


# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "d, expected",
    [
        (0, 1),
        ([1, 2], 2),
        ({}, 0),
        ({"a": 0}, 2),
        ({"a": 0, "b": 1}, 4),
        ({"a": [{"a1": 1, "a2": 2}], "b": {"b1": 1, "b2": 2}}, 11),
    ],
)
def test__count_members(d, expected):
    assert ddl.count_members(d) == expected


@pytest.mark.parametrize(
    "d, expected",
    [
        ({}, {}),
        ([{"a": 1}, {"a": 1, "b": 2}], {"a": 1, "b": 2}),
        (
            [{"a": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 2, "c": {"c1": 1}}],
            {"a": 1, "b": 2, "c": {"c1": 1}},
        ),
    ],
)
def test__loc_dict(d, expected):
    assert ddl.loc_dict(d) == expected
