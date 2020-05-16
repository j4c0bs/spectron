# -*- coding: utf-8 -*-

import argparse
import logging
from pathlib import Path
import sys

try:
    import ujson as json
except ImportError:
    import json

from . import __version__ as version
from . import spectrum_schema


logging.basicConfig(
    level=logging.WARN,
    format="-- [%(asctime)s][%(levelname)s][%(filename)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("spectron")


def str_list_type(input):
    val = []
    if "," in input:
        val.extend((s.strip() for s in input.split(",") if s.strip()))
    else:
        val.append(input.strip())
    return val


def json_type(input):
    d = json.loads(Path(input))
    return tuple(d.items())


def parse_arguments():
    """Parse CLI args."""

    parser = argparse.ArgumentParser(
        description="auto generate Spectrum DDL from JSON",
    )

    parser.set_defaults(partitions=None, mapping=None, type_map=None)

    parser.add_argument("-v", "--version", action="version", version=version)

    parser.add_argument(
        dest="infile",
        type=argparse.FileType("r"),
        nargs="?",
        default=sys.stdin,
        help="JSON to convert",
    )

    parser.add_argument(
        "-c",
        "--convert_hyphens",
        action="store_true",
        dest="convert_hyphens",
        help="auto convert hypens to underscores (adds field to mapping)",
    )

    parser.add_argument(
        "-l",
        "--lowercase",
        action="store_true",
        dest="case_insensitive",
        help="enable case insensitivity and force all fields to lowercase - applied before field lookup in mapping",
    )

    parser.add_argument(
        "-j",
        "--ignore_malformed_json",
        action="store_true",
        dest="ignore_malformed_json",
        help="ignore malformed json",
    )

    parser.add_argument(
        "-e",
        "--error_nested_arrarys",
        action="store_false",
        dest="ignore_nested_arrarys",
        help="ignore nested arrays",
    )

    parser.add_argument(
        "-s", "--schema", type=str, dest="schema", help="Schema name",
    )

    parser.add_argument(
        "-t", "--table", type=str, dest="table", help="Table name",
    )

    parser.add_argument(
        "-p",
        "--partitions_file",
        type=argparse.FileType("r"),
        dest="partitions",
        help="JSON filepath to map parition column(s) e.g. {column: dtype}",
    )

    parser.add_argument(
        "--s3", type=str, dest="s3_key", help="S3 Key prefix e.g. bucket/dir",
    )

    parser.add_argument(
        "-m",
        "--mapping",
        type=argparse.FileType("r"),
        dest="mapping_file",
        help="JSON filepath to use for mapping field names e.g. {field_name: new_field_name}",
    )
    parser.add_argument(
        "-y",
        "--type_map",
        type=argparse.FileType("r"),
        dest="type_map_file",
        help="JSON filepath to use for mapping field names to known data types e.g. {key: value}",
    )

    parser.add_argument(
        "-f",
        "--ignore_fields",
        type=str_list_type,
        dest="ignore_fields",
        help="Comma separated fields to ignore",
    )

    args = parser.parse_args()

    if args.partitions_file:
        args.partitions = json.loads(args.partitions_file.read())

    if args.mapping_file:
        args.mapping = json.loads(args.mapping_file.read())

    if args.type_map_file:
        args.type_map = json.loads(args.type_map_file.read())

    return args


def create_spectrum_schema():
    """Create Spectrum schema from JSON."""

    args = parse_arguments()
    d = json.loads(args.infile.read())
    kwargs = {k: v for (k, v) in args._get_kwargs()}
    statement = spectrum_schema.from_dict(d, **kwargs)
    print(statement)
