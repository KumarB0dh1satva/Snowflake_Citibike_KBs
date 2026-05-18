-- -----------------------------------------------------------
-- V_PENDING_FILES
-- Files that are in the manifest but not yet successfully loaded.
-- Run this before or after the SP to see what is still outstanding.
-- -----------------------------------------------------------
CREATE OR REPLACE VIEW CITIBIKE_SYSTEM_DATA.LOGGING.V_PENDING_FILES AS
SELECT
    m.OUTPUT_FILE,
    m.REGION,
    m.SF_SCHEMA,
    m.SF_TABLE,
    m.GZIP_SIZE_BYTES,
    s.STATUS          AS LAST_STATUS,
    s.ATTEMPT         AS LAST_ATTEMPT,
    s.ERROR_MESSAGE   AS LAST_ERROR,
    s.ENDED_AT_UTC    AS LAST_ATTEMPT_AT
FROM CITIBIKE_SYSTEM_DATA.LOGGING.STAGE_MANIFEST m
LEFT JOIN CITIBIKE_SYSTEM_DATA.LOGGING.V_LATEST_INGEST_STATUS s
       ON s.OUTPUT_FILE = m.OUTPUT_FILE
WHERE COALESCE(s.STATUS, 'PENDING') != 'SUCCESS'
ORDER BY m.REGION, m.SF_TABLE, m.OUTPUT_FILE;