-- -----------------------------------------------------------
-- STAGE_MANIFEST
-- Populated once from your gzip_manifest.jsonl.
-- The SP reads this as its work queue.
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS CITIBIKE_SYSTEM_DATA.LOGGING.STAGE_MANIFEST (
    OUTPUT_FILE         VARCHAR(500)  NOT NULL  COMMENT 'Relative path under gzip_staging/ e.g. nyc/abc123/nyc_abc123_part0001.csv.gz',
    REGION              VARCHAR(50)             COMMENT 'nyc or jersey_city',
    SCHEMA_KEY          VARCHAR(50)             COMMENT 'MD5 schema fingerprint from compress_for_snowflake.py',
    SF_SCHEMA           VARCHAR(100)            COMMENT 'Target Snowflake schema e.g. STAGING_NYC',
    SF_TABLE            VARCHAR(100)            COMMENT 'Target table e.g. TRIPS_MODERN',
    GZIP_SIZE_BYTES     NUMBER                  COMMENT 'Compressed file size in bytes',
    SOURCE_FILE_COUNT   NUMBER                  COMMENT 'Number of source CSVs packed into this gz',
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Prevent duplicate manifest rows
ALTER TABLE CITIBIKE_SYSTEM_DATA.LOGGING.STAGE_MANIFEST
    ADD CONSTRAINT IF NOT EXISTS UQ_STAGE_MANIFEST_FILE
    UNIQUE (OUTPUT_FILE);