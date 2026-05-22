-- ============================================================
-- SP_BUILD_FACT_RIDE_NYC.sql
-- Loads INT_UDM_NYC.FACT_RIDE from STAGING_NYC.TRIPS_ALL
-- Resolves station SKs from DIM_STATION
-- Resolves date SKs from DIM_DATES
-- Derives TRIP_DURATION_MIN, START_HOUR, AGE_AT_RIDE, AGE_BUCKET
-- Idempotent on _SOURCE_FILE + _SOURCE_ROW_NUMBER
-- ============================================================
CREATE OR REPLACE PROCEDURE CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_FACT_RIDE_NYC(
    P_TRUNCATE_FIRST BOOLEAN DEFAULT FALSE
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_inserted INTEGER DEFAULT 0;
BEGIN
 
    IF (P_TRUNCATE_FIRST) THEN
        TRUNCATE TABLE CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.FACT_RIDE;
    END IF;
 
    INSERT INTO CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.FACT_RIDE (
        TRIP_SK_STAGING,
        RIDE_ID,
        START_STATION_SK,
        END_STATION_SK,
        START_DATE_SK,
        END_DATE_SK,
        STARTED_AT,
        ENDED_AT,
        TRIP_DURATION_SEC,
        TRIP_DURATION_MIN,
        START_HOUR,
        RIDEABLE_TYPE,
        BIKEID,
        MEMBER_CASUAL,
        BIRTH_YEAR,
        GENDER,
        AGE_AT_RIDE,
        AGE_BUCKET,
        SOURCE_SCHEMA,
        _SOURCE_FILE,
        _SOURCE_ROW_NUMBER,
        _LOADED_AT
    )
    SELECT
        SRC.TRIP_SK                                         AS TRIP_SK_STAGING,
        SRC.RIDE_ID,
 
        -- Station SK lookup (join on TRIM+UPPER name)
        SS.STATION_SK                                       AS START_STATION_SK,
        ES.STATION_SK                                       AS END_STATION_SK,
 
        -- Date SK lookup (YYYYMMDD integer)
        TO_NUMBER(TO_CHAR(SRC.STARTED_AT::DATE, 'YYYYMMDD'))  AS START_DATE_SK,
        TO_NUMBER(TO_CHAR(SRC.ENDED_AT::DATE,   'YYYYMMDD'))  AS END_DATE_SK,
 
        SRC.STARTED_AT,
        SRC.ENDED_AT,
 
        SRC.TRIP_DURATION                                   AS TRIP_DURATION_SEC,
        ROUND(SRC.TRIP_DURATION / 60.0, 2)                  AS TRIP_DURATION_MIN,
        HOUR(SRC.STARTED_AT)                                AS START_HOUR,
 
        LOWER(TRIM(SRC.RIDEABLE_TYPE))                      AS RIDEABLE_TYPE,
        SRC.BIKEID,
 
        COALESCE(LOWER(TRIM(SRC.MEMBER_CASUAL)), 'unknown')  AS MEMBER_CASUAL,
        SRC.BIRTH_YEAR,
        SRC.GENDER,
 
        -- Age (legacy only; NULL for modern)
        CASE
            WHEN SRC.BIRTH_YEAR IS NOT NULL
             AND SRC.BIRTH_YEAR BETWEEN 1900 AND 2010
            THEN YEAR(SRC.STARTED_AT) - SRC.BIRTH_YEAR
            ELSE NULL
        END                                                 AS AGE_AT_RIDE,
 
        -- Age bucket
        CASE
            WHEN SRC.BIRTH_YEAR IS NULL THEN NULL
            WHEN YEAR(SRC.STARTED_AT) - SRC.BIRTH_YEAR < 18    THEN '<18'
            WHEN YEAR(SRC.STARTED_AT) - SRC.BIRTH_YEAR < 25    THEN '18-24'
            WHEN YEAR(SRC.STARTED_AT) - SRC.BIRTH_YEAR < 35    THEN '25-34'
            WHEN YEAR(SRC.STARTED_AT) - SRC.BIRTH_YEAR < 45    THEN '35-44'
            WHEN YEAR(SRC.STARTED_AT) - SRC.BIRTH_YEAR < 55    THEN '45-54'
            WHEN YEAR(SRC.STARTED_AT) - SRC.BIRTH_YEAR < 65    THEN '55-64'
            ELSE '65+'
        END AS AGE_BUCKET,
        CASE
            WHEN SPLIT_PART(SRC._SOURCE_FILE, '_', 2) = 'dc497b4333c4' THEN 'MODERN'
            WHEN SPLIT_PART(SRC._SOURCE_FILE, '_', 2) = '473144999085' THEN 'LEGACY_V1'
            ELSE 'LEGACY_V2'
        END AS SOURCE_SCHEMA,
        SRC._SOURCE_FILE,
        SRC._SOURCE_ROW_NUMBER,
        CURRENT_TIMESTAMP() AS _LOADED_AT
 
    FROM CITIBIKE_SYSTEM_DATA.STAGING_NYC.TRIPS_ALL SRC
 
    -- Resolve start station SK
    LEFT JOIN CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_STATION SS
        ON SS.STATION_NAME = TRIM(UPPER(SRC.START_STATION_NAME))
 
    -- Resolve end station SK
    LEFT JOIN CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_STATION ES
        ON ES.STATION_NAME = TRIM(UPPER(SRC.END_STATION_NAME))
 
    -- Dedup guard
    WHERE NOT EXISTS (
        SELECT 1
        FROM CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.FACT_RIDE TGT
        WHERE TGT._SOURCE_FILE       = SRC._SOURCE_FILE
          AND TGT._SOURCE_ROW_NUMBER = SRC._SOURCE_ROW_NUMBER
    )
 
    -- Drop bad durations
    AND SRC.TRIP_DURATION BETWEEN 60 AND 86400
 
    -- Drop rows where date SK won't resolve (outside DIM_DATES spine)
    AND SRC.STARTED_AT::DATE BETWEEN '2013-01-01' AND '2030-12-31';
 
    rows_inserted := SQLROWCOUNT;
    RETURN 'SP_BUILD_FACT_RIDE_NYC: inserted ' || rows_inserted || ' rows into INT_UDM_NYC.FACT_RIDE';
 
END;
$$;
