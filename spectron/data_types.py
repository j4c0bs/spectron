# -*- coding: utf-8 -*-

from enum import IntEnum, auto
from functools import singledispatch

from . import parse_date


REDSHIFT_MAP = {
    "int2": "SMALLINT",
    "smallint": "SMALLINT",
    "int": "INT",
    "int4": "INT",
    "integer": "INT",
    "int8": "BIGINT",
    "bigint": "BIGINT",
    "real": "FLOAT4",
    "float": "FLOAT8",
    "double precision": "FLOAT8",
    "boolean": "BOOL",
    "character": "VARCHAR",
    "character varying": "VARCHAR",
    "date": "DATE",
    "timestamp without time zone": "TIMESTAMP",
}

REDSHIFT_ALIAS = set(REDSHIFT_MAP.values())


class Numeric(IntEnum):
    SMALLINT = auto()
    INT = auto()
    BIGINT = auto()
    FLOAT4 = auto()
    FLOAT8 = auto()


DATETIME_TYPES = {"DATE", "TIMESTAMP"}
NUMERIC_TYPES = set(Numeric.__members__.keys())


def largest_numeric_type(*dtypes: str) -> str:
    """Get dtype with largest range."""

    num_type = max((getattr(Numeric, dtype) for dtype in dtypes))
    return num_type.name


def resolve_type(inferred_dtype: str, user_dtype: str) -> str:
    """Detect and resolve data type when inferred != user provided.

    Rules (inferred vs. user defined):
        string / varchar > all:
            - override all dtypes except for date/time
        numeric > all numeric:
            - user defined numeric(x,y) types override int, float
            - other inferred dtypes override user defined
        int < bool:
            - user defined bool replaces inferred int
        int < float:
            - user defined float is used since spectrum supports int to float
        int <-> int:
            - largest int type is used
        float <-> float:
            - largest float type is used
    """

    dtype = None

    if inferred_dtype == "VARCHAR":
        if user_dtype in DATETIME_TYPES:
            dtype = user_dtype
        else:
            dtype = inferred_dtype
    elif "NUMERIC" in user_dtype or "DECIMAL" in user_dtype:
        if inferred_dtype in NUMERIC_TYPES:
            dtype = user_dtype
        else:
            dtype = inferred_dtype
    elif "INT" in inferred_dtype:
        if user_dtype == "BOOL" or "FLOAT" in user_dtype:
            dtype = user_dtype
        elif "INT" in user_dtype:
            dtype = largest_numeric_type(inferred_dtype, user_dtype)
        else:
            dtype = inferred_dtype
    elif "FLOAT" in inferred_dtype:
        if "FLOAT" in user_dtype:
            dtype = largest_numeric_type(inferred_dtype, user_dtype)
        else:
            dtype = inferred_dtype
    else:
        dtype = user_dtype

    return dtype


def type_set(t: list):
    return set(map(type, t))


def num_in_bounds(num_bits, val):
    n_max = (2 ** num_bits) // 2
    if val > 0:
        n_max -= 1
    return abs(val) <= n_max


@singledispatch
def set_dtype(val, **kwargs):
    return f"UNKNOWN_{type(val).__name__}"


@set_dtype.register
def __str_dtype(val: str, infer_date: bool = False, **kwargs):

    if any(c.isdigit() for c in val):
        dtype = parse_date.guess_type(val)
        if dtype:
            return dtype
    return "VARCHAR"


@set_dtype.register
def __float_dtype(val: float, strict: bool = False, **kwargs):

    dtype = "FLOAT8"

    if num_in_bounds(32 - 6, val):
        dtype = "FLOAT4"
    elif num_in_bounds(64 - 15, val):
        dtype = "FLOAT8"
    elif strict:
        raise OverflowError(f"FLOAT exceeds 49 bits: {val}")
    return dtype


@set_dtype.register
def __int_dtype(val: int, strict: bool = False, **kwargs):

    dtype = "BIGINT"

    if num_in_bounds(16, val):
        dtype = "SMALLINT"
    elif num_in_bounds(32, val):
        dtype = "INT"
    elif num_in_bounds(64, val):
        dtype = "BIGINT"
    elif strict:
        raise OverflowError(f"INT exceeds 64 bits: {val}")
    return dtype


@set_dtype.register
def __bool_dtype(val: bool, **kwargs):
    return "BOOL"
