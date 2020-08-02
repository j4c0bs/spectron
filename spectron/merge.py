# -*- coding: utf-8 -*-

from typing import Any, Dict, Generator, List, Optional, Tuple, Union


def pk(parent: Optional[Union[Tuple[str], List[str]]], key: str) -> Tuple[str]:
    """Construct chained parent key as list of strings."""

    if parent:
        return tuple((*parent, key))
    return tuple((key,))


def terminal(v: Any) -> bool:
    """Detect terminal key ending in type other than non-empty dict or list."""

    if isinstance(v, (dict, list)):
        if v:
            return False
    return True


def extract_terminal_keys(
    d: Dict, parent: Optional[List[str]] = None
) -> Generator[Tuple[Tuple[str], Dict], None, None]:
    """Get parent keys that reference a value or no other keys."""

    if isinstance(d, dict):
        for k, v in d.items():
            parent_key = pk(parent, k)
            if terminal(v):
                if isinstance(v, list):
                    yield pk(parent_key, "[array]"), None
                else:
                    yield parent_key, v
            else:
                yield from extract_terminal_keys(v, parent_key)
    elif isinstance(d, list):
        if d:
            for item in d:
                parent_key = pk(parent, "[array]")
                yield from extract_terminal_keys(item, parent_key)
        else:
            parent_key = pk(parent, "[array]")
            yield parent_key, None
    else:
        yield parent, d


# Create Dict --------------------------------------------------------------------------


def split_key(parent_key: List[str]) -> List[List[str]]:
    """Split group key into parts."""

    key_parts = []
    while parent_key:
        key_parts.append(parent_key)
        parent_key = parent_key[:-1]
    return key_parts[::-1]


def construct_branch(
    d: Dict, loc: Dict, group_key: Tuple[str], is_dict: bool = False, key_val=None
):
    """Construct dict branch from terminal parent key."""

    key_parts = split_key(group_key)
    array_keys = [k for k in key_parts if k[-1] == "[array]"]
    array_parents = [k[:-1] for k in array_keys]

    for key_part in key_parts:
        if key_part in loc:
            continue

        # detect top level key first
        if len(key_part) == 1:
            key = key_part[0]

            if key_part == group_key:
                val = {} if is_dict else key_val
            elif key_part in array_parents:
                val = []
            else:
                val = {}

            d[key] = val
            loc[key_part] = d[key]
        else:
            par_key, key = key_part[:-1], key_part[-1]
            ref = loc[par_key]

            if key_part in array_parents:
                # child key_part ends in dict ref or [array]
                # array parents create list + point to list
                # implies non-terminal key
                val = []
                if par_key in array_parents:
                    # reference is list
                    # some_key.[array].[array]
                    if key_part in array_keys:
                        # some_key.[array].[array]
                        ref.append(val)
                        loc[key_part] = loc[par_key][0]
                    else:
                        # some_key.[array]
                        sub_d = {key: val}
                        if ref:
                            ref[0].update(sub_d)
                        else:
                            ref.append(sub_d)
                        loc[key_part] = ref[0][key]
                elif par_key in array_keys:
                    if key_part in array_keys:
                        loc[key_part] = ref
                    else:
                        sub_d = {key: val}
                        if ref:
                            ref[0].update(sub_d)
                        else:
                            ref.append(sub_d)
                        loc[key_part] = ref[0][key]
                else:
                    # some_key.[array] == {k: []}
                    sub_d = {key: val}
                    ref.update(sub_d)
                    loc[key_part] = ref[key]

            elif key_part in array_keys:
                # append to parent ref
                if key_part == group_key:
                    # terminal
                    if key_val is not None:
                        ref.append(key_val)
                else:
                    loc[key_part] = ref
            elif key_part == group_key:
                # assign key_val
                sub_d = {key: key_val}
                if par_key in array_keys:
                    if ref:
                        ref[0].update(sub_d)
                    else:
                        ref.append(sub_d)
                else:
                    ref.update(sub_d)
            else:
                val = {}
                sub_d = {key: val}
                if par_key in array_keys:
                    if ref:
                        ref[0].update(sub_d)
                    else:
                        ref.append(sub_d)
                    ref = ref[0]
                else:
                    ref.update(sub_d)

                loc[key_part] = ref[key]

    return d, loc


def reconstruct_dict(key_store: Dict) -> Dict:
    """Construct dict from key store."""

    d, loc = {}, {}

    for group_key, field in sorted(key_store.items(), key=lambda t: t[0]):

        val = field.max_value
        is_dict = isinstance(val, dict)
        if val == []:
            val = None

        construct_branch(d, loc, group_key, is_dict=is_dict, key_val=val)

    return d, loc
