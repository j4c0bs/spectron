# -*- coding: utf-8 -*-

import json
import pytest

from spectron.spectrum_schema import (
    conform_syntax,
    define_types,
    format_definitions,
    from_dict,
)


def test_full_run(datadir):

    input_path = datadir / "test_1_line.json"
    result_path = datadir / "test_1_line.sql"

    d = json.loads(input_path.read_text())
    assert from_dict(d).strip() == result_path.read_text().strip()


@pytest.mark.parametrize("d, expected", [])
def test_conform_syntax(d, expected):
    assert conform_syntax(d) == expected


@pytest.mark.parametrize(
    "d, mapping, type_map, ignore_fields, convert_hyphens, case_insensitive, ignore_nested_arrarys, expected",
    [],
)
def test_define_types(
    d,
    mapping,
    type_map,
    ignore_fields,
    convert_hyphens,
    case_insensitive,
    ignore_nested_arrarys,
    expected,
):
    assert (
        define_types(
            d,
            mapping,
            type_map,
            ignore_fields,
            convert_hyphens,
            case_insensitive,
            ignore_nested_arrarys,
        )
        == expected
    )


@pytest.mark.parametrize(
    "d, mapping, type_map, ignore_fields, convert_hyphens, case_insensitive, expected",
    [],
)
def test_format_definitions(
    d, mapping, type_map, ignore_fields, convert_hyphens, case_insensitive, expected
):
    assert (
        format_definitions(
            d, mapping, type_map, ignore_fields, convert_hyphens, case_insensitive
        )
        == expected
    )


@pytest.mark.parametrize(
    "d, mapping, type_map, ignore_fields, convert_hyphens, schema, table, partitions, s3_key, case_insensitive, ignore_malformed_json, kwargs, expected",
    [],
)
def test_from_dict(
    d,
    mapping,
    type_map,
    ignore_fields,
    convert_hyphens,
    schema,
    table,
    partitions,
    s3_key,
    case_insensitive,
    ignore_malformed_json,
    kwargs,
    expected,
):
    assert (
        from_dict(
            d,
            mapping,
            type_map,
            ignore_fields,
            convert_hyphens,
            schema,
            table,
            partitions,
            s3_key,
            case_insensitive,
            ignore_malformed_json,
            kwargs,
        )
        == expected
    )
