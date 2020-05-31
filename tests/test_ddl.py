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


# Test Define Types --------------------------------------------------------------------


@pytest.mark.parametrize(
    "d, kwargs, expected",
    [
        ({"a": 1}, {}, ({"a": "SMALLINT"}, {})),
        ({"a": 1}, {"mapping": {"a": "x"}}, ({"x": "SMALLINT"}, {"x": "a"})),
        ({"a": 1}, {"type_map": {"a": "TEST_TYPE"}}, ({"a": "TEST_TYPE"}, {})),
        ({"a": 1, "b": 1}, {"ignore_fields": {"b"}}, ({"a": "SMALLINT"}, {})),
        ({"a-x": 1}, {"convert_hyphens": True}, ({"a_x": "SMALLINT"}, {"a_x": "a-x"})),
        ({"a-x": 1}, {"convert_hyphens": False}, ({"`a-x`": "SMALLINT"}, {})),
        (
            {"caseKey": 1},
            {"case_map": True},
            ({"casekey": "SMALLINT"}, {"casekey": "caseKey"}),
        ),
        ({"caseKey": 1}, {"case_map": False}, ({"caseKey": "SMALLINT"}, {}),),
        ({"caseKey": 1}, {"case_insensitive": True}, ({"casekey": "SMALLINT"}, {}),),
        ({"caseKey": 1}, {"case_insensitive": False}, ({"caseKey": "SMALLINT"}, {}),),
        ({"a": [1, "mixed-dtypes"]}, {}, ({}, {}),),
        (
            {"a": {"a1": ["not-nested"]}, "b": [["nested"]]},
            {},
            ({"a": {"a1": ["VARCHAR"]}}, {}),
        ),
        ({"avg": "reserved-keyword"}, {}, ({"`avg`": "VARCHAR"}, {}),),
        (
            {"A": 1},
            {"type_map": {"A": "TEST_TYPE"}, "case_map": True},
            ({"a": "TEST_TYPE"}, {"a": "A"}),
        ),
    ],
)
def test__define_types(d, kwargs, expected):
    assert ddl.define_types(d, **kwargs) == expected


# Test Key Ops -------------------------------------------------------------------------

str_128_chars = "x" * 128


@pytest.mark.parametrize(
    "key, expected", [("0", False), ("x", True), ("_x", True), ("_0x", True),],
)
def test__valid_identifier(key, expected):
    assert ddl.validate_identifier(key) == expected


@pytest.mark.parametrize(
    "key", [" ", "has'-'quotes", str_128_chars],
)
def test__valid_identifier__exceptions(key):
    with pytest.raises(ValueError):
        ddl.validate_identifier(key)


# Test Conform Key ---------------------------------------------------------------------

user_map = {
    "table": "src_table",
    "Table": "DEST_TABLE",
    "col-x": "0-col-x",
    "x-z-1": "1x-z",
    "0x": "0x_invalid_key",
    "1x": "1x-invalid-map",
}


@pytest.mark.parametrize(
    "key, mapping, case_map, convert_hyphens, case_insensitive, expected",
    [
        ("x", {}, False, False, False, ("x", None)),
        ("x", {}, True, True, False, ("x", None)),
        # case_map
        ("X", {}, True, False, False, ("X", "x")),
        # case_insensitive
        ("X", {}, False, False, True, ("x", None)),
        # case_map + convert_hyphens
        ("Col-Hyphen", {}, True, True, False, ("Col-Hyphen", "col_hyphen")),
        # # check hyphens
        ("col-x", {}, False, False, False, ("`col-x`", None),),
        ("col-x", {}, False, True, False, ("col-x", "col_x"),),
        ("col-x", user_map, False, False, False, ("col-x", "`_0-col-x`"),),
        ("col-x", user_map, False, True, False, ("col-x", "_0_col_x"),),
        ("col-x", user_map, True, False, False, ("col-x", "`_0-col-x`"),),
        # check reserved keyword
        ("table", None, False, False, False, ("`table`", None)),
        ("table", user_map, False, False, False, ("table", "src_table")),
        ("table", user_map, True, True, False, ("table", "src_table")),
        # check case options with user map
        ("Table", None, False, False, False, ("`Table`", None)),
        ("Table", user_map, False, False, False, ("Table", "DEST_TABLE")),
        ("Table", user_map, True, False, False, ("Table", "dest_table")),
        ("Table", user_map, False, True, False, ("Table", "DEST_TABLE")),
        ("Table", user_map, False, False, True, ("table", "dest_table")),
        # # prefix invalid key with underscore
        ("0x", user_map, False, False, False, ("0x", "_0x_invalid_key")),
        ("1x", user_map, False, False, False, ("1x", "`_1x-invalid-map`")),
    ],
)
def test__conform_key(
    key, mapping, case_map, convert_hyphens, case_insensitive, expected
):

    # sanity check
    assert not (case_map and case_insensitive)

    proc_key, mapped_key = ddl.conform_key(
        key, mapping, case_map, convert_hyphens, case_insensitive
    )

    expected_proc_key, expected_mapped_key = expected
    assert proc_key == expected_proc_key
    assert mapped_key == expected_mapped_key
