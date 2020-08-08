# -*- coding: utf-8 -*-

import logging
import sys

from functools import partial
from itertools import takewhile

try:
    import ujson as json
except ImportError:
    import json

from . import data_types
from . import reserved
from . import write_ddl


logger = logging.getLogger(__name__)

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


# Keys ---------------------------------------------------------------------------------


def _as_parent_key(parent, key):
    """Construct parent key."""

    if parent:
        return f"{parent}.{key}"
    return key


def validate_identifier(key: str):
    """Confirm key is valid identifier.
        - between 1 and 127 bytes in length
        - does not contain quotation marks
        - starts with alphabetic or underscore character
            * does not raise error, fixed in mapping
    """

    key = key.strip()

    if not key:
        raise ValueError("Column name is empty string...")

    if len(key) > 127:
        raise ValueError("Column name exceeds 127 characters...")

    if "'" in key or '"' in key:
        raise ValueError("Column name contains quotation marks...")

    return key[0].isalpha() or key.startswith("_")


def detect_reserved_key(key, parent):
    if key.strip("`").lower() in reserved.keywords:
        logger.info(f"Reserved keyword detected: {parent}.{key}")


def detect_hyphens(key: str, convert_hyphens: bool):
    """Detect hyphens and enclose in quotes if located."""

    if "-" in key:
        if convert_hyphens:
            key = key.replace("-", "_")
        else:
            key = f"`{key}`"
    return key


def conform_key(
    key: str,
    mapping: dict,
    case_map: bool,
    convert_hyphens: bool,
    case_insensitive: bool,
):
    """Conform key to user options and standard identifier rules."""

    if case_insensitive and case_map:
        raise ValueError("case_insensitive and case_map both True...")

    mapped_key = None

    use_key_map = mapping and key in mapping
    is_reserved = key.lower() in reserved.keywords

    if use_key_map:
        mapped_key = mapping[key]

    if is_reserved and not mapped_key:
        if case_insensitive:
            key = key.lower()
        key = f"`{key}`"
        return key, mapped_key

    # apply case fold to source column and user defined mapped key
    if case_map:
        if mapped_key:
            if any(c.isupper() for c in mapped_key):
                mapped_key = mapped_key.lower()
        elif any(c.isupper() for c in key):
            mapped_key = key.lower()
    elif case_insensitive:
        key = key.lower()
        if mapped_key:
            mapped_key = mapped_key.lower()

    # check for invalid keys and mutate key used in DDL
    if mapped_key:
        if not validate_identifier(mapped_key):
            mapped_key = f"_{mapped_key}"
    elif not validate_identifier(key):
        mapped_key = f"_{key}"

    # override user map and enclose hyphenated column name in quotes
    if mapped_key:
        mapped_key = detect_hyphens(mapped_key, convert_hyphens)
    else:
        proc_key = detect_hyphens(key, convert_hyphens)
        if proc_key.strip("`") != key:
            mapped_key = proc_key
        else:
            key = proc_key

    return key, mapped_key


def process_keys(
    parent,
    keys,
    mapping,
    ignore_fields,
    case_map,
    convert_hyphens,
    case_insensitive,
    **kwargs,
):
    """Apply key transforms and detect conflicts."""

    conform = partial(
        conform_key,
        mapping=mapping,
        case_map=case_map,
        convert_hyphens=convert_hyphens,
        case_insensitive=case_insensitive,
    )

    keys = [k for k in keys if not (ignore_fields and k in ignore_fields)]
    proc_keys = {key: conform(key) for key in keys}

    new_keys = [k[1] if k[1] is not None else k[0] for k in proc_keys.values()]

    # detect key conflict
    if len(set(new_keys)) < len(keys):
        conflicts = sorted(set(k for k in new_keys if new_keys.count(k) > 1))
        raise ValueError(f"Key conflicts in {parent}: {', '.join(conflicts)}")

    return proc_keys


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


def validate_array(array, parent, ignore_nested_arrarys):
    """Confirm array has single data type, no empty or nested arrays."""

    if not array:
        logger.warning(f"[{parent}] Ignoring empty array")
        return False

    # confirm single dtype
    _num_dtypes = len(data_types.type_set(array))
    if _num_dtypes > 1:
        logger.warning(
            f"[{parent}] Ignoring array with multiple dtypes ({_num_dtypes})"
        )
        return False

    # check for nested arrays
    if any(isinstance(item, list) for item in array):
        if ignore_nested_arrarys:
            logger.warning(f"[{parent}] Ignoring nested arrays ({len(array)})")
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
    numeric_overflow=False,
):
    """Replace values with data types and maintain data structure."""

    key_map = {}  # dict of confirmed keys to include in serde mapping

    kwargs = {
        "mapping": mapping,
        "type_map": type_map,
        "ignore_fields": ignore_fields,
        "infer_date": infer_date,
        "convert_hyphens": convert_hyphens,
        "case_map": case_map,
        "case_insensitive": case_insensitive,
        "ignore_nested_arrarys": ignore_nested_arrarys,
    }

    set_dtype = partial(
        data_types.set_dtype, infer_date=infer_date, strict=numeric_overflow
    )

    def parse_types(d, parent=None):
        """Crawl and assign data types."""

        nonlocal key_map

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

            proc_keys = process_keys(parent, d.keys(), **kwargs)

            for key, val in d.items():
                if ignore_fields and key in ignore_fields:
                    continue

                dtype = None
                parent_key = _as_parent_key(parent, key)
                ref_key, mapped_key = proc_keys[key]

                # set user defined dtype
                if type_map and key in type_map:
                    dtype = type_map[key].strip()

                    if dtype.lower() in data_types.REDSHIFT_MAP:
                        dtype = data_types.REDSHIFT_MAP[dtype.lower()]
                    elif dtype.upper() in data_types.REDSHIFT_ALIAS:
                        dtype = dtype.upper()

                # add mapping to key_map
                if mapped_key:
                    key_map[mapped_key] = ref_key
                    key = mapped_key
                else:
                    key = ref_key

                # determine dtype and generate new dict
                if isinstance(val, (dict, list)):
                    dtype = parse_types(val, parent=parent_key)
                    if dtype:
                        as_types[key] = dtype
                else:
                    inferred_dtype = set_dtype(val)

                    # detect dtype mismatch between inferred and user provided
                    if dtype and dtype != inferred_dtype:
                        user_dtype = dtype[:]
                        dtype = data_types.resolve_type(inferred_dtype, user_dtype)

                        if dtype != user_dtype:
                            logger.warning(
                                f"[{parent_key}] Using {dtype} instead of {user_dtype}"
                            )
                    else:
                        dtype = inferred_dtype

                    # skip keys with unknown data types and log
                    if "UNKNOWN" in dtype:
                        _str_dtype = dtype.split("_", 1)[1]
                        logger.warning(
                            f"Unknown dtype {_str_dtype} for {parent_key}: {val}"
                        )
                    else:
                        as_types[key] = dtype

        else:
            as_types = set_dtype(d)

        return as_types

    return parse_types(d), key_map


# --------------------------------------------------------------------------------------


def format_definitions(d, **kwargs):
    """Format field names and set dtypes."""

    with_types, key_map = define_types(d, **kwargs)

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
    numeric_overflow=False,
    **kwargs,
):
    """Create Spectrum schema from dict."""

    validate_input(d)

    definitions, key_map = format_definitions(
        d,
        mapping=mapping,
        type_map=type_map,
        ignore_fields=ignore_fields,
        convert_hyphens=convert_hyphens,
        case_map=case_map,
        case_insensitive=case_insensitive,
        ignore_nested_arrarys=ignore_nested_arrarys,
        numeric_overflow=numeric_overflow,
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
