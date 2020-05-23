![Upload Python Package](https://github.com/j4c0bs/spectron/workflows/.github/workflows/pythonpublish.yml/badge.svg)

# [WIP] spectron

Generate AWS Athena and Spectrum DDL from JSON


## Install:

```
pip install spectron[json]

```


## CLI Usage:

```
spectron nested_big_data.json > nested_big_data.sql
```

---

```
positional arguments:
  infile                JSON to convert

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  -c, --case_map        disable case insensitivity and map field with
                        uppercase chars to lowercase
  -l, --lowercase       DDL: enable case insensitivity and force all fields to
                        lowercase - applied before field lookup in mapping
  -r, --retain_hyphens  disable auto convert hypens to underscores
  -e, --error_nested_arrarys
                        raise exception for nested arrays
  -f IGNORE_FIELDS, --ignore_fields IGNORE_FIELDS
                        Comma separated fields to ignore
  -j, --ignore_malformed_json
                        DDL: ignore malformed json
  -m MAPPING_FILE, --mapping MAPPING_FILE
                        JSON filepath to use for mapping field names e.g.
                        {field_name: new_field_name}
  -y TYPE_MAP_FILE, --type_map TYPE_MAP_FILE
                        JSON filepath to use for mapping field names to known
                        data types e.g. {column: dtype}
  -p PARTITIONS_FILE, --partitions_file PARTITIONS_FILE
                        DDL: JSON filepath to map parition column(s) e.g.
                        {column: dtype}
  -s SCHEMA, --schema SCHEMA
                        DDL: schema name
  -t TABLE, --table TABLE
                        DDL: table name
  --s3 S3_KEY           DDL: S3 Key prefix e.g. bucket/dir
```

## Options:

**TODO**

---

## Programmatic Usage:

```python

In [1]: from spectron import ddl                                                

In [2]: %paste                                                                  
d = {
    "uuid": 1234567,
    "events": [
        {"ts": 0, "status": True, "avg": 0.123},
        {"ts": 1, "status": False, "avg": 1.234}
    ]
}

In [3]: sql = ddl.from_dict(d)                                                  

In [4]: print(sql)                                                              
CREATE EXTERNAL TABLE {schema}.{table} (
    uuid INT,
    events array<
        struct<
            ts: SMALLINT,
            status: BOOL,
            "avg": FLOAT4
        >
    >
)
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'case.insensitive'='FALSE',
    'ignore.malformed.json'='TRUE'
)
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION 's3://{bucket}/{prefix}';

```

---
