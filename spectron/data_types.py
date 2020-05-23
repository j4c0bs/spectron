# -*- coding: utf-8 -*-

from functools import singledispatch


def type_set(t: list):
    return set(map(type, t))


def int_in_bounds(num_bits, val):
    n_max = (2 ** num_bits) // 2
    if val > 0:
        n_max -= 1
    return abs(val) <= n_max


@singledispatch
def set_dtype(val):
    return f"UNKNOWN_{type(val).__name__}"


@set_dtype.register
def __str_dtype(val: str):
    return "VARCHAR"


@set_dtype.register
def __float_dtype(val: float):

    num_bits = (64 - 15, 32 - 6)
    dtypes = ("FLOAT8", "FLOAT4")

    dtype = None
    for n, dtype in zip(num_bits, dtypes):
        if abs(val) >= 2 ** n // 2:
            break
    return dtype


@set_dtype.register
def __int_dtype(val: int):

    if int_in_bounds(16, val):
        dtype = "SMALLINT"
    elif int_in_bounds(32, val):
        dtype = "INT"
    elif int_in_bounds(64, val):
        dtype = "BIGINT"
    else:
        raise ValueError(f"Input exceeds integer max number of bits: {val}")
    return dtype


@set_dtype.register
def __bool_dtype(val: bool):
    return "BOOL"
