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


def as_parent_key(parent, key):
    """Construct parent key."""

    if parent:
        return f"{parent}.{key}"
    return key


def inspect_array(array, parent, ignore_nested_arrarys):
    """Check for nested arrays and report."""

    num_arrays = sum(isinstance(item, list) for item in array)

    if num_arrays:
        if ignore_nested_arrarys:
            logger.warn(f"Skipping nested arrays ({num_arrays}) in {parent}...")
        else:
            msg = f"Nested arrays detected ({num_arrays}) in {parent}..."
            raise ValueError(msg)


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

    def check_key_map(key):
        nonlocal key_map

        if (mapping and key in mapping) or ("-" in key and convert_hyphens):
            if key in mapping:
                new_key = mapping[key]
            else:
                new_key = key.replace("-", "_")

            key_map[new_key] = key
            return new_key
        return key

    def parse_types(d, parent=None):

        if isinstance(d, list):
            as_types = []

            parent_key = as_parent_key(parent, "array")
            inspect_array(d, parent_key, ignore_nested_arrarys)

            for item in d:
                if isinstance(item, list):
                    continue
                as_types.append(parse_types(item, parent=parent_key))

        elif isinstance(d, dict):
            as_types = {}
            for key, val in d.items():
                if key in ignore_fields:
                    continue

                parent_key = as_parent_key(parent, key)

                if case_insensitive:
                    key = key.lower()

                key = check_key_map(key)

                # replace back ticks with quotes after formatting
                if not convert_hyphens and "-" in key:
                    key = f"`{key}`"

                if key in reserved.keywords:
                    logger.info(f"Enclosing reserved keyword in quotes: {key}")
                    key = f"`{key}`"

                if isinstance(val, (dict, list)):
                    as_types[key] = parse_types(val, parent=parent_key)
                else:
                    if type_map and key in type_map:
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
    ignore_nested_arrarys=True,
):
    """Format field names and set dtypes."""

    with_types, key_map = define_types(
        d,
        mapping=mapping,
        type_map=type_map,
        ignore_fields=ignore_fields,
        convert_hyphens=convert_hyphens,
        case_insensitive=case_insensitive,
        ignore_nested_arrarys=ignore_nested_arrarys,
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
    ignore_nested_arrarys=True,
    **kwargs,
):
    """Create Spectrum schema from dict."""

    definitions, key_map = format_definitions(
        d,
        mapping,
        type_map,
        ignore_fields,
        convert_hyphens,
        case_insensitive,
        ignore_nested_arrarys,
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
