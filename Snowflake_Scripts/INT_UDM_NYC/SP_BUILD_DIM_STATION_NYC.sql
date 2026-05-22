-- ============================================================
-- SP_BUILD_DIM_STATION_NYC.sql
-- Resolves unique stations from STAGING_NYC.TRIPS_ALL
-- Uses station NAME as master key (IDs are inconsistent across eras)
-- Uses MEDIAN coordinates to suppress GPS noise
-- ============================================================

CREATE OR REPLACE PROCEDURE CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_DIM_STATION_NYC()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_inserted INTEGER DEFAULT 0;
BEGIN

    TRUNCATE TABLE CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_STATION;

    INSERT INTO CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_STATION (
        STATION_NAME,
        LEGACY_STATION_ID,
        MODERN_STATION_ID,
        STATION_LAT,
        STATION_LNG,
        IN_LEGACY,
        IN_MODERN,
        AVAILABILITY,
        TRIP_OBSERVATIONS,
        LAT_VARIANCE,
        LNG_VARIANCE,
        _CREATED_AT,
        _UPDATED_AT
    )
    WITH

    -- --------------------------------------------------------
    -- Collect all station name + id + coordinate observations
    -- from both start and end sides of every trip
    -- --------------------------------------------------------
    ALL_OBSERVATIONS AS (
        SELECT
            TRIM(UPPER(START_STATION_NAME))     AS STATION_NAME,
            START_STATION_ID                    AS STATION_ID,
            START_LAT                           AS LAT,
            START_LNG                           AS LNG,
            SOURCE_SCHEMA
        FROM CITIBIKE_SYSTEM_DATA.STAGING_NYC.TRIPS_ALL
        WHERE START_STATION_NAME IS NOT NULL
          AND START_LAT BETWEEN 40.0 AND 42.0
          AND START_LNG BETWEEN -75.0 AND -72.0

        UNION ALL

        SELECT
            TRIM(UPPER(END_STATION_NAME)),
            END_STATION_ID,
            END_LAT,
            END_LNG,
            SOURCE_SCHEMA
        FROM CITIBIKE_SYSTEM_DATA.STAGING_NYC.TRIPS_ALL
        WHERE END_STATION_NAME IS NOT NULL
          AND END_LAT BETWEEN 40.0 AND 42.0
          AND END_LNG BETWEEN -75.0 AND -72.0
    ),

    -- --------------------------------------------------------
    -- Separate legacy vs modern IDs per station name
    -- --------------------------------------------------------
    LEGACY_IDS AS (
        SELECT STATION_NAME, MAX(STATION_ID) AS LEGACY_STATION_ID
        FROM ALL_OBSERVATIONS
        WHERE SOURCE_SCHEMA IN ('LEGACY_V1', 'LEGACY_V2')
        GROUP BY STATION_NAME
    ),

    MODERN_IDS AS (
        SELECT STATION_NAME, MAX(STATION_ID) AS MODERN_STATION_ID
        FROM ALL_OBSERVATIONS
        WHERE SOURCE_SCHEMA = 'MODERN'
        GROUP BY STATION_NAME
    ),

    -- --------------------------------------------------------
    -- Aggregate coordinates and stats per station name
    -- --------------------------------------------------------
    COORDS AS (
        SELECT
            STATION_NAME,
            MEDIAN(LAT)                         AS STATION_LAT,
            MEDIAN(LNG)                         AS STATION_LNG,
            MAX(LAT) - MIN(LAT)                 AS LAT_VARIANCE,
            MAX(LNG) - MIN(LNG)                 AS LNG_VARIANCE,
            COUNT(*)                            AS TRIP_OBSERVATIONS,
            MAX(CASE WHEN SOURCE_SCHEMA IN ('LEGACY_V1','LEGACY_V2') THEN 1 ELSE 0 END) = 1 AS IN_LEGACY,
            MAX(CASE WHEN SOURCE_SCHEMA = 'MODERN'                   THEN 1 ELSE 0 END) = 1 AS IN_MODERN
        FROM ALL_OBSERVATIONS
        GROUP BY STATION_NAME
    )

    SELECT
        C.STATION_NAME,
        L.LEGACY_STATION_ID,
        M.MODERN_STATION_ID,
        C.STATION_LAT,
        C.STATION_LNG,
        C.IN_LEGACY,
        C.IN_MODERN,
        CASE
            WHEN C.IN_LEGACY AND C.IN_MODERN THEN 'BOTH'
            WHEN C.IN_LEGACY                 THEN 'LEGACY_ONLY'
            ELSE                                  'MODERN_ONLY'
        END                                         AS AVAILABILITY,
        C.TRIP_OBSERVATIONS,
        ROUND(C.LAT_VARIANCE, 6)                    AS LAT_VARIANCE,
        ROUND(C.LNG_VARIANCE, 6)                    AS LNG_VARIANCE,
        CURRENT_TIMESTAMP()                         AS _CREATED_AT,
        CURRENT_TIMESTAMP()                         AS _UPDATED_AT

    FROM COORDS C
    LEFT JOIN LEGACY_IDS L ON L.STATION_NAME = C.STATION_NAME
    LEFT JOIN MODERN_IDS M ON M.STATION_NAME = C.STATION_NAME
    ORDER BY C.STATION_NAME;

    rows_inserted := SQLROWCOUNT;
    RETURN 'SP_BUILD_DIM_STATION_NYC: inserted ' || rows_inserted || ' stations into INT_UDM_NYC.DIM_STATION';

END;
$$;
