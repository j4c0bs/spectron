# -*- coding: utf-8 -*-

import logging

from collections import defaultdict
from itertools import chain
from typing import Dict

logger = logging.getLogger(__name__)


def add_hist_dicts(d1: Dict, d2: Dict) -> Dict:
    d = defaultdict(int)
    for k, v in chain(d1.items(), d2.items()):
        d[k] += v
    return d


class Field:
    """Tracks max value and data type(s)."""

    _max_int = 2 ** 64 // 2
    _max_float = 2 ** (65 - 14) // 2
    numeric_types = {"int", "float"}

    def __init__(self, parent_key: str, value=None, *, str_numeric_override=False):
        self.parent_key = parent_key
        self.str_numeric_override = str_numeric_override
        self.num_na = 0
        self.dtype_max = {}
        self.hist = defaultdict(int)
        self.add(value)

    def __add__(self, other):
        str_numeric_override = any(
            (self.str_numeric_override, other.str_numeric_override)
        )
        field = Field(self.parent_key, str_numeric_override=str_numeric_override)
        field.num_na = self.num_na + other.num_na

        for dtype, value in chain(self.dtype_max.items(), other.dtype_max.items()):
            field._update_dtype_max(dtype, value)

        hist = add_hist_dicts(self.hist, other.hist)
        field.hist.update(hist)
        return field

    def push_warnings(self):
        """Detect and log mixed dtypes, int overflow."""

        if isinstance(self.parent_key, tuple):
            par_key = ".".join(self.parent_key)
        else:
            par_key = self.parent_key

        if len(self.hist.keys()) > 1:
            logger.warning(
                f"[{par_key}] dtypes detected {', '.join(sorted(self.hist.keys()))}"
            )

        for str_type, str_ref, num_max in zip(
            ("int", "float"),
            ("BIGINT (2**64)", "FLOAT8 (2**(65 - 14)//2)"),
            (self._max_int, self._max_float),
        ):
            if str_type in self.dtype_max and abs(self.dtype_max[str_type]) >= num_max:
                logger.warning(
                    f"[{par_key}] {str_type} exceeds max {str_ref}: {self.dtype_max[str_type]}"
                )

    @property
    def dtype(self):
        """Get data type conforming to spectrum requirements or with highest count.

        If `str_numeric_override` is enabled and any strings have been seen, returned
        dtype is forced as str.

        If int and float have been seen, dtype defaults to float.
        """

        dtype = None
        if self.hist:

            if len(self.hist.keys()) == 1:
                dtype = list(self.hist.keys())[0]
            else:
                numeric_intersect = self.numeric_types & self.hist.keys()

                if numeric_intersect:
                    if self.str_numeric_override and "str" in self.hist:
                        dtype = "str"
                    elif "float" in numeric_intersect:
                        dtype = "float"

            if not dtype:
                dtype, _ = max(self.hist.items(), key=lambda t: t[1])

        return dtype

    def _get_max_comparable(self, prev_value, value, key_func):
        """Get max value for inputs which can be compared."""

        max_value = None
        if prev_value is None:
            max_value = value
        else:
            max_value = max(value, prev_value, key=key_func)
        return max_value

    def _compare_numeric(self, prev_value, value):
        return self._get_max_comparable(prev_value, value, abs)

    def _compare_str(self, prev_value, value):
        return self._get_max_comparable(prev_value, value, len)

    def _compare_other_types(self, prev_value, value):
        return value

    @property
    def max_value(self):
        """Get max value for current dtype."""

        _dtype = self.dtype
        is_numeric = _dtype in self.numeric_types

        if is_numeric and self.numeric_types.issubset(self.hist.keys()):
            val = self._compare_numeric(self.dtype_max["float"], self.dtype_max["int"])
            return float(val)

        return self.dtype_max.get(_dtype)

    def _update_dtype_max(self, incoming_dtype, value):
        """Detect dtype change and store diffs."""

        if incoming_dtype in self.dtype_max:
            prev_value = self.dtype_max[incoming_dtype]

            comp_func = None
            if incoming_dtype == "str":
                comp_func = self._compare_str
            elif incoming_dtype in self.numeric_types:
                comp_func = self._compare_numeric
            else:
                comp_func = self._compare_other_types

            self.dtype_max[incoming_dtype] = comp_func(prev_value, value)

        else:
            self.dtype_max[incoming_dtype] = value

    def add(self, value):
        """Add value to field and track dtype."""

        if value is not None:
            incoming_dtype = type(value).__name__
            self._update_dtype_max(incoming_dtype, value)
            self.hist[incoming_dtype] += 1
        else:
            self.num_na += 1
