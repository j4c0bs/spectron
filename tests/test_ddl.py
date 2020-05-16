# -*- coding: utf-8 -*-

import pytest

from spectron.ddl import create_statement, create_table, indent_quoted, set_options


@pytest.mark.parametrize(
    "definitions, key_map, schema, table, partitions, s3_key, case_insensitive, ignore_malformed_json, expected",
    [],
)
def test_create_statement(
    definitions,
    key_map,
    schema,
    table,
    partitions,
    s3_key,
    case_insensitive,
    ignore_malformed_json,
    expected,
):
    assert (
        create_statement(
            definitions,
            key_map,
            schema,
            table,
            partitions,
            s3_key,
            case_insensitive,
            ignore_malformed_json,
        )
        == expected
    )


@pytest.mark.parametrize("definitions, schema, table, expected", [])
def test_create_table(definitions, schema, table, expected):
    assert create_table(definitions, schema, table) == expected


@pytest.mark.parametrize("s, n, expected", [])
def test_indent_quoted(s, n, expected):
    assert indent_quoted(s, n) == expected


@pytest.mark.parametrize(
    "key_map, partitions, s3_key, case_insensitive, ignore_malformed_json, expected", []
)
def test_set_options(
    key_map, partitions, s3_key, case_insensitive, ignore_malformed_json, expected
):
    assert (
        set_options(
            key_map, partitions, s3_key, case_insensitive, ignore_malformed_json
        )
        == expected
    )
