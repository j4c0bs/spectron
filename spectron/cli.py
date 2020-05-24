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
from . import ddl


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
        description="Generate Athena and Spectrum DDL from JSON",
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

    case_group = parser.add_mutually_exclusive_group()

    case_group.add_argument(
        "-c",
        "--case_map",
        action="store_true",
        dest="case_map",
        help="disable case insensitivity and map field with uppercase chars to lowercase",
    )

    case_group.add_argument(
        "-l",
        "--lowercase",
        action="store_true",
        dest="case_insensitive",
        help="DDL: enable case insensitivity and force all fields to lowercase - applied before field lookup in mapping",
    )

    parser.add_argument(
        "-d",
        "--infer_date",
        action="store_true",
        help="infer date string types - supports ISO 8601 for date, datetime[TZ]",
    )

    parser.add_argument(
        "-r",
        "--retain_hyphens",
        action="store_false",
        dest="convert_hyphens",
        help="disable auto convert hypens to underscores",
    )

    parser.add_argument(
        "-e",
        "--error_nested_arrarys",
        action="store_false",
        dest="ignore_nested_arrarys",
        help="raise exception for nested arrays",
    )

    parser.add_argument(
        "-f",
        "--ignore_fields",
        type=str_list_type,
        dest="ignore_fields",
        metavar="col1,col2,...",
        help="Comma separated fields to ignore",
    )

    parser.add_argument(
        "-m",
        "--mapping",
        type=argparse.FileType("r"),
        dest="mapping_file",
        metavar="filepath",
        help="JSON filepath to use for mapping field names e.g. {field_name: new_field_name}",
    )
    parser.add_argument(
        "-y",
        "--type_map",
        type=argparse.FileType("r"),
        dest="type_map_file",
        metavar="filepath",
        help="JSON filepath to use for mapping field names to known data types e.g. {key: value}",
    )

    parser.add_argument(
        "-p",
        "--partitions_file",
        type=argparse.FileType("r"),
        dest="partitions_file",
        metavar="filepath",
        help="DDL: JSON filepath to map parition column(s) e.g. {column: dtype}",
    )

    parser.add_argument(
        "-j",
        "--ignore_malformed_json",
        action="store_true",
        dest="ignore_malformed_json",
        help="DDL: ignore malformed json",
    )
    parser.add_argument(
        "-s",
        "--schema",
        type=str,
        dest="schema",
        metavar="schema",
        help="DDL: schema name",
    )

    parser.add_argument(
        "-t",
        "--table",
        type=str,
        dest="table",
        metavar="table",
        help="DDL: table name",
    )

    parser.add_argument(
        "--s3",
        type=str,
        dest="s3_key",
        metavar="s3://bucket/key",
        help="DDL: S3 Key prefix",
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
    statement = ddl.from_dict(d, **kwargs)
    print(statement)
