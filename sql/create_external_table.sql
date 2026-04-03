CREATE EXTERNAL SCHEMA clickstream_gcs
FROM DATA CATALOG
DATABASE 'clickstream_external'
IAM_ROLE 'arn:aws:iam::123456789012:role/YourRedshiftRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

CREATE EXTERNAL TABLE clickstream_gcs.events (
    event_id VARCHAR(256),
    user_id BIGINT,
    event_timestamp TIMESTAMP,
    page_url VARCHAR(1024),
    product_id BIGINT
)
PARTITIONED BY (event_date DATE, event_type VARCHAR(50))
STORED AS PARQUET
LOCATION 's3://your-s3-bucket/processed/';
