# -*- coding: utf-8 -*-

from textwrap import indent


SERDE_FORMAT = "org.openx.data.jsonserde.JsonSerDe"
INPUTFORMAT = "org.apache.hadoop.mapred.TextInputFormat"
OUTPUTFORMAT = "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"


def indent_quoted(s, n=1):
    spc = " " * n * 4
    return indent(f"'{s}'", spc)


def create_table(definitions, schema=None, table=None):
    """Construct `create table` statement in DDL.

    Arg:
        definitions (str):
            - field name : dtype map
    Kwargs:
        schema (str):
            - default: None
            - schema to template
        table (str):
            - default: None
            - table name to template

    Returns: str
    """

    if not schema:
        schema = "{schema}"
    if not table:
        table = "{table}"

    create = f"CREATE EXTERNAL TABLE {schema}.{table}"

    definitions = definitions.strip("\n")
    if not definitions.startswith(" "):
        definitions = indent(definitions, "    ")
    return f"{create} (\n{definitions}\n)"


def set_options(
    key_map=None,
    partitions=None,
    s3_key=None,
    case_insensitive=True,
    ignore_malformed_json=True,
):
    """Construct options statement in DDL.

    Kwargs:
        key_map (dict):
            - default: None
            - mapping for for column names
        partitions (list(str)):
            - default: None
            - [(column, dtype),] partitions
        s3_key (str):
            - default: None
            - S3 key prefix
        case_insensitive (bool):
            - default: True
            - set all fields to lower case
        ignore_malformed_json (bool):
            - default: True
            - instruct Spectrum to ignore bad JSON in data lake

    Returns: str
    """

    bool_str = lambda b: "TRUE" if b else "FALSE"

    if not s3_key:
        s3_key = "s3://{bucket}/{prefix}"

    # Partitions
    if partitions:
        key_types = (f"{key} {dtype.upper()}" for (key, dtype) in partitions)
        partition_by = f"PARTITIONED BY ({', '.join(key_types)})"
    else:
        partition_by = ""

    # OpenX Serde Properties
    serde_properties = []
    if key_map:
        mappings = [f"'mapping.{key}'='{val}'" for key, val in key_map.items()]
        serde_properties.extend(mappings)

    case_option = f"'case.insensitive'='{bool_str(case_insensitive)}'"
    serde_properties.append(case_option)
    json_option = f"'ignore.malformed.json'='{bool_str(ignore_malformed_json)}'"
    serde_properties.append(json_option)
    serde_properties = ",\n".join(serde_properties)

    statement = f"""
{partition_by}
ROW FORMAT SERDE
{indent_quoted(SERDE_FORMAT)}
WITH SERDEPROPERTIES (
{indent(serde_properties, '    ')}
)
STORED AS INPUTFORMAT
{indent_quoted(INPUTFORMAT)}
OUTPUTFORMAT
{indent_quoted(OUTPUTFORMAT)}
LOCATION '{s3_key}';"""

    return statement.strip()


def create_statement(
    definitions,
    key_map=None,
    schema=None,
    table=None,
    partitions=None,
    s3_key=None,
    case_insensitive=True,
    ignore_malformed_json=True,
):
    """Construct Spectrum DDL.

    Arg:
        definitions (str):
            - field name : dtype map
    Kwargs:
        schema (str):
            - default: None
            - schema to template
        table (str):
            - default: None
            - table name to template
        key_map (dict):
            - default: None
            - mapping for for column names
        partitions (list(str)):
            - default: None
            - [(column, dtype),] partitions
        s3_key (str):
            - default: None
            - S3 key prefix
        case_insensitive (bool):
            - default: True
            - set all fields to lower case
        ignore_malformed_json (bool):
            - default: True
            - instruct Spectrum to ignore bad JSON in data lake

    Returns: str
    """

    create_external_table = create_table(definitions, schema, table)

    options = set_options(
        key_map=key_map,
        partitions=partitions,
        s3_key=s3_key,
        case_insensitive=case_insensitive,
        ignore_malformed_json=ignore_malformed_json,
    )

    return "\n".join((create_external_table, options))
