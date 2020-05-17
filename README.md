# [WIP] spectron

Generate AWS Spectrum DDL from JSON


## CLI Usage:

```
spectron -lj nested_big_data.json > nested_big_data.sql
```

---

```
auto generate Spectrum DDL from JSON

positional arguments:
  infile                JSON to convert

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  -r, --retain_hyphens  disable auto convert hypens to underscores
  -l, --lowercase       enable case insensitivity and force all fields to
                        lowercase - applied before field lookup in mapping
  -e, --error_nested_arrarys
                        raise exception for nested arrays
  -m MAPPING_FILE, --mapping MAPPING_FILE
                        JSON filepath to use for mapping field names e.g.
                        {field_name: new_field_name}
  -y TYPE_MAP_FILE, --type_map TYPE_MAP_FILE
                        JSON filepath to use for mapping field names to known
                        data types e.g. {key: value}
  -f IGNORE_FIELDS, --ignore_fields IGNORE_FIELDS
                        Comma separated fields to ignore
  -p PARTITIONS_FILE, --partitions_file PARTITIONS_FILE
                        DDL: JSON filepath to map parition column(s) e.g.
                        {column: dtype}
  -j, --ignore_malformed_json
                        DDL: ignore malformed json
  -s SCHEMA, --schema SCHEMA
                        DDL: schema name
  -t TABLE, --table TABLE
                        DDL: table name
  --s3 S3_KEY           DDL: S3 Key prefix e.g. bucket/dir
  ```
