# -*- coding: utf-8 -*-

import logging

from collections import defaultdict
from typing import Dict, List, Tuple

from . import data_types
from .merge import construct_branch, extract_terminal_keys


logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------------------


class Field:
    """Tracks `max` value and data type(s)."""

    numeric_types = {"int", "float"}

    def __init__(self, parent_key: str, value=None):
        self.parent_key = parent_key
        self.num_na = 0
        self.dtype_change = {}
        self.max_value = None
        self.is_numeric = False
        self._add_next_value = None
        self.hist = defaultdict(int)
        self._dtype = self._set_dtype(value)
        self.add(value)

    @property
    def dtype(self):
        return self._dtype

    def _set_dtype(self, value):
        if value is not None:
            v_dtype = type(value).__name__
            if v_dtype in self.numeric_types:
                self.is_numeric = True
                self._add_next_value = self._add_numeric
            else:
                self.is_numeric = False
                if v_dtype == "str":
                    self._add_next_value = self._add_str
                else:
                    self._add_next_value = self._identity
            return v_dtype
        return None

    def _store_dtype_change(self, value):

        _prev_value = self.max_value

        if self.dtype_change:
            if self._dtype in self.dtype_change:
                prev = self.dtype_change[self._dtype]
                if self._dtype == "str":
                    _prev_value = max(prev, self.max_value)
                elif self._dtype in self.numeric_types:
                    _prev_value = max(prev, abs(self.max_value))

        self.dtype_change[self._dtype] = _prev_value
        self._dtype = self._set_dtype(value)
        self.max_value = None

    def _valid_type(self, value):
        """Detect dtype change and store diffs."""

        v_dtype = type(value).__name__

        if self._dtype:
            if v_dtype == self._dtype:
                return True
            elif v_dtype in self.numeric_types and self.is_numeric:
                return True
            else:
                self._store_dtype_change(value)
        else:
            self._dtype = self._set_dtype(value)
        return False

    def add(self, value):

        if value is not None:
            self._valid_type(value)
            self._add_next_value(value)
            self.hist[self._dtype] += 1
        else:
            self.num_na += 1

    def _add_comparable(self, value, key_func):
        if self.max_value is not None:
            self.max_value = max(value, self.max_value, key=key_func)
        else:
            self.max_value = value

    def _add_numeric(self, value):
        self._add_comparable(value, abs)

    def _add_str(self, value):
        self._add_comparable(value, len)

    def _identity(self, value):
        self.max_value = value


# --------------------------------------------------------------------------------------


class MaxDict:
    """Collect and store field, `max` value per key branch."""

    def __init__(self):
        self.hist = defaultdict(int)
        self.key_store = {}

    def add(self, key: str, value):
        self.hist[key] += 1
        if key in self.key_store:
            self.key_store[key].add(value)
        else:
            self.key_store[key] = Field(key, value)

    def load_dict(self, d: Dict):
        for key, value in extract_terminal_keys(d):
            self.add(key, value)

    def batch_load_dicts(self, items: List[Dict]):
        for d in items:
            self.load_dict(d)

    def fields(self):
        yield from self.key_store.values()

    def fields_seen(self) -> Tuple[int, int]:
        tot = 0
        tot_na = 0
        for field in self.fields():
            tot += sum(field.hist.values())
            tot_na += field.num_na
        return tot, tot_na

    def has_dtype_changes(self) -> List[Field]:
        loc = []
        for field in self.fields():
            if field.dtype_change:
                loc.append(field)
        return loc

    def asdict(self, astype: bool = False) -> Dict:
        """Returned keys, [max | type] vals as dict."""

        d, loc = {}, {}
        for group_key, field in sorted(self.key_store.items(), key=lambda t: t[0]):
            is_dict = False
            val = field.max_value

            if val is None:
                logger.warning(f"Ignoring key with None value: {'.'.join(group_key)}")
                continue

            if isinstance(val, dict):
                is_dict = True
            elif isinstance(val, list):
                if val and val[0] is not None:
                    val = val.pop(0)
                else:
                    val = []
            elif astype:
                val = data_types.set_dtype(val)

            construct_branch(d, loc, group_key, is_dict=is_dict, key_val=val)

        return d
