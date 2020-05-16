# -*- coding: utf-8 -*-

from itertools import takewhile
import logging

try:
    import ujson as json
except ImportError:
    import json

from . import data_types
from . import ddl
from . import reserved


logger = logging.getLogger("spectron")

# --------------------------------------------------------------------------------------


def count_indent(line):
    for _ in takewhile(lambda c: c == " ", line):
        yield 1


def strip_top_level_seps(s):
    lines = []
    for line in s.split("\n"):
        if sum(count_indent(line)) == 4:
            line = line.replace(":", " ")
        lines.append(line)
    return "\n".join(lines)


def conform_syntax(d):
    """Replace Python syntax to match Spectrum DDL syntax."""

    s = json.dumps(d, indent=4).strip()

    # remove outermost brackets
    s = s[1:][:-1]
    s = s.rstrip()

    # replace dict, lists with schema dtypes
    s = s.replace("{", "struct<").replace("}", ">")
    s = s.replace("[", "array<").replace("]", ">")

    # drop colons in top level fields
    s = strip_top_level_seps(s)

    # add space after colon
    s = s.replace(":", ": ")

    # drop quotes, replace back-ticks
    s = s.replace('"', "").replace("`", '"')
    return s


# --------------------------------------------------------------------------------------


def define_types(
    d,
    mapping=None,
    type_map=None,
    ignore_fields=None,
    convert_hyphens=False,
    case_insensitive=False,
    ignore_nested_arrarys=True,
):
    """Replace values with data types and maintain data structure."""

    if not mapping:
        mapping = {}

    if not type_map:
        type_map = {}

    if not ignore_fields:
        ignore_fields = {}

    key_map = {}  # dict of confirmed keys to include in serde mapping

    def parse_types(d):
        nonlocal key_map

        if isinstance(d, list):
            as_types = []
            for item in d:
                if isinstance(item, list):
                    if ignore_nested_arrarys:
                        # # TODO: get parent field for reference
                        # logger.warn(f"Skipping nested array...")
                        continue
                    else:
                        raise ValueError("Array types cannot contain arrays...")
                as_types.append(parse_types(item))

        elif isinstance(d, dict):
            as_types = {}
            for key, val in d.items():

                if key in ignore_fields:
                    continue

                if case_insensitive:
                    key = key.lower()

                if key in mapping or ("-" in key and convert_hyphens):
                    if key in mapping:
                        new_key = mapping[key]
                    else:
                        new_key = key.replace("-", "_")

                    key_map[new_key] = key
                    key = new_key

                # replace back ticks with quotes after formatting
                if not convert_hyphens and "-" in key:
                    key = f"`{key}`"

                if key in reserved.keywords:
                    logger.info(f"Enclosing reserved keyword in quotes: {key}")
                    key = f"`{key}`"

                if isinstance(val, (dict, list)):
                    as_types[key] = parse_types(val)
                else:
                    if key in type_map:
                        dtype = type_map[key]
                    else:
                        dtype = data_types.set_dtype(val)

                    if "UNKNOWN" in dtype:
                        logger.warn(f"Unknown dtype for {key}: {val}")

                    as_types[key] = dtype

        else:
            as_types = data_types.set_dtype(d)

        return as_types

    return parse_types(d), key_map


# --------------------------------------------------------------------------------------


def format_definitions(
    d,
    mapping=None,
    type_map=None,
    ignore_fields=None,
    convert_hyphens=False,
    case_insensitive=False,
):
    """Format field names and set dtypes."""

    with_types, key_map = define_types(
        d,
        mapping=mapping,
        ignore_fields=ignore_fields,
        convert_hyphens=convert_hyphens,
        case_insensitive=case_insensitive,
    )

    definitions = conform_syntax(with_types)
    return definitions, key_map


def from_dict(
    d,
    mapping=None,
    type_map=None,
    ignore_fields=None,
    convert_hyphens=False,
    schema=None,
    table=None,
    partitions=None,
    s3_key=None,
    case_insensitive=True,
    ignore_malformed_json=True,
    **kwargs,
):
    """Create Spectrum schema from dict."""

    definitions, key_map = format_definitions(
        d, mapping, type_map, ignore_fields, convert_hyphens, case_insensitive
    )

    statement = ddl.create_statement(
        definitions,
        key_map,
        schema,
        table,
        partitions,
        s3_key,
        case_insensitive,
        ignore_malformed_json,
    )

    return statement
