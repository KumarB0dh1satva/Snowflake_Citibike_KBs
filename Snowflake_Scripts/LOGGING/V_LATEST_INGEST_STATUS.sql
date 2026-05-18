-- -----------------------------------------------------------
-- V_LATEST_INGEST_STATUS
-- One row per file — the most recent attempt only.
-- Use this to see what is done, pending, or failed.
-- -----------------------------------------------------------
CREATE OR REPLACE VIEW CITIBIKE_SYSTEM_DATA.LOGGING.V_LATEST_INGEST_STATUS AS
SELECT
    il.*
FROM CITIBIKE_SYSTEM_DATA.LOGGING.INGEST_LOG il
INNER JOIN (
    SELECT OUTPUT_FILE, MAX(LOG_ID) AS MAX_LOG_ID
    FROM   CITIBIKE_SYSTEM_DATA.LOGGING.INGEST_LOG
    GROUP BY OUTPUT_FILE
) latest ON il.LOG_ID = latest.MAX_LOG_ID;