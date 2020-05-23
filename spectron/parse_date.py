# -*- coding: utf-8 -*-

from dateutil.parser import parse
from pendulum import parse as strict_parse
from pendulum.exceptions import ParserError


def num_digits(s: str):
    return sum(c.isdigit() for c in s)


def guess_type(s: str):
    """Guess datetime data type for input string. Supports ISO 8601. 

    Uses pendulum parser to determine if input is valid date(time).
    Due to pendulum including tzinfo in all datetime objects, dateutil parser is used
    to determine if TZ is included.
    """

    n = num_digits(s)

    # ISO 8601 dates have at least 8 digits
    if n < 8:
        return None

    try:
        strict_parse(s)
    except ValueError:
        return None
    except ParserError:
        return None

    # detect date
    if n == 8:
        dtype = "DATE"
    else:
        dt = parse(s)

        if dt.tzinfo:
            dtype = "TIMESTAMPTZ"
        else:
            dtype = "TIMESTAMP"

    return dtype
