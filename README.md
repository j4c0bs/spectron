![Upload Python Package](https://github.com/j4c0bs/spectron/workflows/.github/workflows/pythonpublish.yml/badge.svg?branch>=0.2.4)
![pip](https://img.shields.io/pypi/v/spectron.svg?style=flat-square)


# [WIP] spectron

Generate AWS Athena and Spectrum DDL from JSON


## Install:

```
pip install spectron[json]

```


## CLI Usage:

```
# single input file:
spectron nested_big_data.json > nested_big_data.sql

# multiple input files to summarize all key structures seen:
spectron dataz/*.json > big_data.sql
```

---

```
usage: spectron [-h] [-V] [-v] [-c | -l] [-n] [-d] [-r] [-e]
                [-f col1,col2,...] [-m filepath] [-y filepath] [-p filepath]
                [-j] [-s schema] [-t table] [--s3 s3://bucket/key]
                infile [infile ...]

Generate Athena and Spectrum DDL from JSON

positional arguments:
  infile                JSON file(s) to convert

optional arguments:
  -h, --help            show this help message and exit
  -V, --version         show program's version number and exit
  -v, --verbose         increase logging level
  -c, --case_map        disable case insensitivity and map field with
                        uppercase chars to lowercase
  -l, --lowercase       DDL: enable case insensitivity and force all fields to
                        lowercase - applied before field lookup in mapping
  -n, --numeric_overflow
                        raise exception on numeric overflow
  -d, --infer_date      infer date string types - supports ISO 8601 for date,
                        datetime[TZ]
  -r, --retain_hyphens  disable auto convert hypens to underscores
  -e, --error_nested_arrarys
                        raise exception for nested arrays
  -f col1,col2,..., --ignore_fields col1,col2,...
                        Comma separated fields to ignore
  -m filepath, --mapping filepath
                        JSON filepath to use for mapping field names e.g.
                        {field_name: new_field_name}
  -y filepath, --type_map filepath
                        JSON filepath to use for mapping field names to known
                        data types e.g. {key: value}
  -p filepath, --partitions_file filepath
                        DDL: JSON filepath to map parition column(s) e.g.
                        {column: dtype}
  -j, --ignore_malformed_json
                        DDL: ignore malformed json
  -s schema, --schema schema
                        DDL: schema name
  -t table, --table table
                        DDL: table name
  --s3 s3://bucket/key  DDL: S3 Key prefix
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
