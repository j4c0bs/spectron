# -*- coding: utf-8 -*-

from itertools import takewhile
import logging
import sys

try:
    import ujson as json
except ImportError:
    import json

from . import data_types
from . import write_ddl
from . import reserved


logger = logging.getLogger("spectron")

# --------------------------------------------------------------------------------------


def _count_indent(line):
    for _ in takewhile(lambda c: c == " ", line):
        yield 1


def strip_top_level_seps(s):
    lines = []
    for line in s.split("\n"):
        if sum(_count_indent(line)) == 4:
            line = line.replace(":", " ")
        lines.append(line)
    return "\n".join(lines)


def _conform_syntax(d):
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


def count_members(d):
    total = 0
    if d is not None:
        if isinstance(d, dict):
            total += len(d.keys())
            for v in d.values():
                total += count_members(v)
        elif isinstance(d, list):
            total += len(d)
            for item in d:
                if isinstance(item, dict):
                    total += count_members(item)
        else:
            total += 1
    return total


def _as_parent_key(parent, key):
    """Construct parent key."""

    if parent:
        return f"{parent}.{key}"
    return key


def validate_array(array, parent, ignore_nested_arrarys):
    """Confirm array has single data type, no empty or nested arrays."""

    if not array:
        logger.warning(f"Skipping empty array in {parent}...")
        return False

    # confirm single dtype
    if len(data_types.type_set(array)) > 1:
        logger.warning(f"Skipping array with multiple dtypes in {parent}...")
        return False

    # check for nested arrays
    if any(isinstance(item, list) for item in array):
        if ignore_nested_arrarys:
            logger.warning(f"Skipping nested arrays ({len(array)}) in {parent}...")
            return False
        else:
            msg = f"Nested arrays detected ({len(array)}) in {parent}..."
            raise ValueError(msg)

    return True


def define_types(
    d,
    mapping=None,
    type_map=None,
    ignore_fields=None,
    infer_date=False,
    convert_hyphens=False,
    case_map=False,
    case_insensitive=False,
    ignore_nested_arrarys=True,
):
    """Replace values with data types and maintain data structure."""

    key_map = {}  # dict of confirmed keys to include in serde mapping

    def check_key_map(key, parent):
        """Check if key is in user defined map or has underscores converted."""

        nonlocal key_map

        use_key_map = mapping and key in mapping
        use_case_map = case_map and any(c.isupper() for c in key)
        replace_hyphens = convert_hyphens and "-" in key

        if use_key_map or use_case_map or replace_hyphens:
            if use_key_map:
                mapped_key = mapping[key]
            elif use_case_map:
                mapped_key = key.lower()
            else:
                mapped_key = key.replace("-", "_")

            key_map[mapped_key] = key
            key = mapped_key

        else:
            if case_insensitive:
                key = key.lower()

            # replace back ticks with quotes after formatting
            if not convert_hyphens and "-" in key:
                key = f"`{key}`"

            # reserved keywords must be enclosed in double quotes
            if key.lower() in reserved.keywords:
                logger.info(f"Reserved keyword detected: {parent}.{key}")
                key = f"`{key}`"

        return key

    def parse_types(d, parent=None):
        """Crawl and assign data types."""

        if isinstance(d, list):
            as_types = []
            parent_key = _as_parent_key(parent, "array")

            if not validate_array(d, parent_key, ignore_nested_arrarys):
                return None

            if any(isinstance(item, dict) for item in d):
                if len(d) == 1:
                    single_dict = d[0]
                else:
                    single_dict = sorted(d, key=count_members, reverse=True)[0]

                as_types.append(parse_types(single_dict, parent=parent_key))

            else:
                for item in d:
                    if isinstance(item, list):
                        continue
                    as_types.append(parse_types(item, parent=parent_key))
                as_types = sorted(set(as_types))

        elif isinstance(d, dict):
            as_types = {}
            for key, val in d.items():
                if ignore_fields and key in ignore_fields:
                    continue

                dtype = None
                parent_key = _as_parent_key(parent, key)

                # set user defined dtype
                if type_map and key in type_map:
                    dtype = type_map[key]

                key = check_key_map(key, parent)

                if isinstance(val, (dict, list)):
                    dtype = parse_types(val, parent=parent_key)
                    if dtype:
                        as_types[key] = dtype
                else:
                    if not dtype:
                        dtype = data_types.set_dtype(val, infer_date=infer_date)

                    # skip keys with unknown data types and log
                    if "UNKNOWN" in dtype:
                        _str_dtype = dtype.split("_", 1)[1]
                        logger.warning(
                            f"Unknown dtype {_str_dtype} for {parent_key}.{key}: {val}"
                        )
                    else:
                        as_types[key] = dtype

        else:
            as_types = data_types.set_dtype(d, infer_date=infer_date)

        return as_types

    return parse_types(d), key_map


# --------------------------------------------------------------------------------------


def format_definitions(
    d,
    mapping=None,
    type_map=None,
    ignore_fields=None,
    infer_date=False,
    convert_hyphens=False,
    case_map=False,
    case_insensitive=False,
    ignore_nested_arrarys=True,
):
    """Format field names and set dtypes."""

    with_types, key_map = define_types(
        d,
        mapping=mapping,
        type_map=type_map,
        ignore_fields=ignore_fields,
        infer_date=infer_date,
        convert_hyphens=convert_hyphens,
        case_map=case_map,
        case_insensitive=case_insensitive,
        ignore_nested_arrarys=ignore_nested_arrarys,
    )

    if not with_types:
        logger.warning("Aborting - input does not contain valid data structures...")
        sys.exit(1)

    definitions = _conform_syntax(with_types)
    return definitions, key_map


def validate_input(d):
    """Check input type is dict|list."""

    if not d:
        raise ValueError("Input is empty...")

    if not isinstance(d, (dict, list)):
        raise ValueError("Invalid input type...")


def loc_dict(d):
    """Locate largest dict if list(dict) provided."""

    if isinstance(d, list):
        if all(isinstance(item, dict) for item in d):
            if len(d) > 1:
                d = sorted(d, key=count_members, reverse=True)[0]
            else:
                d = d[0]
        else:
            raise ValueError("Input list contains dtypes other than dict...")

    return d


def from_dict(
    d,
    mapping=None,
    type_map=None,
    ignore_fields=None,
    infer_date=False,
    convert_hyphens=False,
    schema=None,
    table=None,
    partitions=None,
    s3_key=None,
    case_map=False,
    case_insensitive=False,
    ignore_malformed_json=True,
    ignore_nested_arrarys=True,
    **kwargs,
):
    """Create Spectrum schema from dict."""

    validate_input(d)
    d = loc_dict(d)

    definitions, key_map = format_definitions(
        d,
        mapping,
        type_map,
        ignore_fields,
        convert_hyphens,
        case_map,
        case_insensitive,
        ignore_nested_arrarys,
    )

    statement = write_ddl.create_statement(
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
