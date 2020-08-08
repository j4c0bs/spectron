# -*- coding: utf-8 -*-

from pendulum import parse


def num_digits(s: str):
    return sum(c.isdigit() for c in s)


def guess_type(s: str):
    """Guess datetime data type for input string. Supports ISO 8601.
    Uses pendulum parser to determine if input is valid date(time).
    """

    n = num_digits(s)

    # ISO 8601 dates have at least 8 digits
    if n < 8:
        return None

    try:
        parse(s)
    except:  # noqa: E722
        return None

    if n == 8:
        dtype = "DATE"
    else:
        dtype = "TIMESTAMP"
    return dtype
