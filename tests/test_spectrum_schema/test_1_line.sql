CREATE EXTERNAL TABLE {schema}.{table} (
    a SMALLINT,
    b BOOL,
    c struct<
        d: SMALLINT
    >
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