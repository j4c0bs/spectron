CREATE EXTERNAL TABLE {schema}.{table} (
    uid VARCHAR,
    data struct<
        "user": VARCHAR,
        events: array<
            struct<
                "timestamp": SMALLINT,
                a: BOOL
            >
        >,
        nested_reserved: struct<
            "column": SMALLINT,
            "table": SMALLINT
        >
    >,
    "test-underscore" SMALLINT
)
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'case.insensitive'='TRUE',
    'ignore.malformed.json'='TRUE'
)
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION 's3://{bucket}/{prefix}';
