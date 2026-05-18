-- -----------------------------------------------------------
-- INGEST_LOG
-- One row per COPY attempt per file.
-- All attempts are kept — latest status per file is the view of record.
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS CITIBIKE_SYSTEM_DATA.LOGGING.INGEST_LOG (
    LOG_ID              NUMBER        AUTOINCREMENT PRIMARY KEY,
    OUTPUT_FILE         VARCHAR(500)  NOT NULL  COMMENT 'Matches STAGE_MANIFEST.OUTPUT_FILE',
    REGION              VARCHAR(50)             COMMENT 'nyc or jersey_city',
    SCHEMA_KEY          VARCHAR(50),
    SF_SCHEMA           VARCHAR(100),
    SF_TABLE            VARCHAR(100),
    GZIP_SIZE_MB        FLOAT,
    ATTEMPT             INT           DEFAULT 1 COMMENT 'Attempt number for this file (1-based)',
    TRIGGERED_BY        VARCHAR(100)            COMMENT 'SP_INGEST_STAGED_FILES | manual | airflow | task',
    STARTED_AT_UTC      TIMESTAMP_NTZ,
    ENDED_AT_UTC        TIMESTAMP_NTZ,
    STATUS              VARCHAR(20)             COMMENT 'SUCCESS | FAILED | SKIPPED',
    ROWS_LOADED         NUMBER,
    ERRORS_SEEN         NUMBER,
    COPY_STATUS_RAW     VARCHAR(500)            COMMENT 'Raw status string(s) from COPY result',
    ERROR_MESSAGE       VARCHAR(4000),
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);