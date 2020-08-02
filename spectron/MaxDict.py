# -*- coding: utf-8 -*-

import logging

from collections import defaultdict
from itertools import chain
from typing import AbstractSet, Dict, Generator, List, Optional, Tuple

from . import data_types
from .Field import Field, add_hist_dicts
from .merge import construct_branch, extract_terminal_keys

logger = logging.getLogger(__name__)

# Mixed array group utils --------------------------------------------------------------


def get_array_parents(field_key: Tuple[str]) -> Generator[Tuple[str], None, None]:
    """Get all array parent keys."""

    ix = 0
    for _ in range(field_key.count("[array]")):
        ix = field_key.index("[array]", ix)
        yield field_key[:ix]
        ix += 1


def loc_siblings(
    parent_key: Tuple[str], keys: List[Tuple[str]], is_array: Optional[bool] = None
) -> List[Tuple[str]]:
    """Locate Field keys which share a parent key."""

    if is_array is not None:
        if is_array:
            comp = lambda k: k == "[array]"
        else:
            comp = lambda k: k != "[array]"
    else:
        comp = lambda k: True

    res = []
    num_parents = len(parent_key)
    for k in keys:
        if len(k) > num_parents:
            if k[:num_parents] == parent_key and comp(k[num_parents]):
                res.append(k)
    return res


def is_parent(parent_key: Tuple[str], key: Tuple[str]) -> bool:
    """Determines if keys have parent:child relationship.

    Example using dot notation:
        is_parent(a.b, a.b.c) == True
        is_parent(a.b, a.b.c.d) == True
        is_parent(a.b, a.x.y) == False

    """

    return len(key) > len(parent_key) and key[: len(parent_key)] == parent_key


# --------------------------------------------------------------------------------------


class MaxDict:
    """Collect and store field, `max` value per key branch.

    When generating a dict, mixed data types and mixed array keys are resolved.

    Data types:
        For fields which have seen string and numeric, priority is determined via:
            str > float > int

    Array keys:
        If a parent key contains both array and non-array child keys, the number of
        values seen for all downstream branches are summed and compared. The key group
        with the highest count will override the other. If counts are equal, array keys
        are prioritized.

        Example using dot notation:

            terminal keys:
                - a.b.[array].d.e = 10
                - a.b.[array].d.f = 10
                - a.b.c.x = 5
                - a.b.c.y = 5

            Count of a.b.[array] = 20, count of a.b.c = 10.
            The output of MaxDict.asdict() will contain a.b.[array] and not a.b.c
    """

    def __init__(self, str_numeric_override: bool = False):
        self.str_numeric_override = str_numeric_override
        self.hist = defaultdict(int)
        self.key_store = {}
        self.override_keys = None

    def __add__(self, other):
        str_numeric_override = any(
            (self.str_numeric_override, other.str_numeric_override)
        )
        md = MaxDict(str_numeric_override=str_numeric_override)
        md.hist.update(add_hist_dicts(self.hist, other.hist))

        for key in self.key_store.keys() & other.key_store.keys():
            md.key_store[key] = self.key_store[key] + other.key_store[key]

        for key in self.key_store.keys() ^ other.key_store.keys():
            if key in self.key_store:
                field = self.key_store[key]
            else:
                field = other.key_store[key]

            md.key_store[key] = field + Field(key)  # coerce new instance

        return md

    def add(self, key: str, value):
        self.hist[key] += 1
        if key in self.key_store:
            self.key_store[key].add(value)
        else:
            self.key_store[key] = Field(
                key, value, str_numeric_override=self.str_numeric_override
            )

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
        """Resolved keys, [max | type] vals as dict."""

        self.load_override_keys()

        d, loc = {}, {}
        for group_key, field in sorted(self.key_store.items(), key=lambda t: t[0]):

            if group_key in self.override_keys:
                continue

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

    # detect mixed terminal, array - dict keys -----------------------------------------

    def sum_key_groups(self, parent_key: Tuple[str]) -> Dict:
        """Sums Field value counts in array, non-array branch groups."""

        key_groups = {
            "array": {"keys": [], "total": 0},
            "non_array": {"keys": [], "total": 0},
        }

        par_ix = len(parent_key)
        for key, field in self.key_store.items():
            if is_parent(parent_key, key):
                count = sum(field.hist.values())

                if key[par_ix] == "[array]":
                    group_ref = key_groups["array"]
                else:
                    group_ref = key_groups["non_array"]

                group_ref["total"] += count
                group_ref["keys"].append(key)

        return key_groups

    def detect_mixed_array_parents(
        self, keys: List[Tuple[str]]
    ) -> AbstractSet[Tuple[str]]:
        """Detect parent keys which have array and non-array children.

        Example using dot notation:

            terminal keys:
                - a.b.[array].d
                - a.b.c.d
            mixed array parent:
                - a.b
        """

        array_keys = [k for k in keys if "[array]" in k]
        array_parents = sorted(
            set(chain(*map(get_array_parents, array_keys))), key=lambda t: len(t)
        )

        mixed_array_parents = set()
        for parent_key in array_parents:
            non_array_siblings = loc_siblings(parent_key, keys, is_array=False)
            if non_array_siblings:
                mixed_array_parents.add(parent_key)

        return mixed_array_parents

    def detect_mixed_terminal_keys(self, keys: List[Tuple[str]]) -> List[Tuple[str]]:
        """Detect terminal keys which are also parent keys.

        Example using dot notation:
            - a.b (added to override_keysride)
            - a.b.c
        """

        max_num_keys = max(len(k) for k in keys)
        mixed_terminal_keys = []

        for parent_key in (k for k in keys if len(k) < max_num_keys):
            parent_key_len = len(parent_key)
            for child_key in (k for k in keys if len(k) > parent_key_len):
                if is_parent(parent_key, child_key):
                    mixed_terminal_keys.append(parent_key)
                    break

        return sorted(mixed_terminal_keys, key=lambda t: len(t))

    def load_override_keys(self):
        """Inspect mixed array groups and select group with highest count.

        If counts are equal, array keys take precedence and non-array keys are ignored
        when constructing dict.
        """

        self.override_keys = []
        keys = sorted(self.key_store.keys(), key=lambda t: len(t))
        mixed_terminal_keys = self.detect_mixed_terminal_keys(keys)

        if mixed_terminal_keys:
            self.override_keys.extend(mixed_terminal_keys)

            # filter terminal keys before detecting mixed array keys
            keys = [k for k in keys if k not in mixed_terminal_keys]

            for terminal_key in mixed_terminal_keys:
                logger.warning(f"Terminal key detected: {'.'.join(terminal_key)}")

        mixed_array_parents = self.detect_mixed_array_parents(keys)
        for parent_key in sorted(mixed_array_parents, key=lambda t: len(t)):
            key_groups = self.sum_key_groups(parent_key)

            pk_ref = ".".join(parent_key)
            num_array = key_groups["array"]["total"]
            num_non_array = key_groups["non_array"]["total"]

            if num_array >= num_non_array:
                self.override_keys.extend(key_groups["non_array"]["keys"])

                logger.warning(
                    f"Array keys override non-arrays: {num_array:,} >= {num_non_array:,} : {pk_ref}"
                )
            else:
                self.override_keys.extend(key_groups["array"]["keys"])

                logger.warning(
                    f"Non-Array keys override arrays: {num_non_array:,} >= {num_array:,} : {pk_ref}"
                )

        self.override_keys = set(self.override_keys)
