-- -----------------------------------------------------------
-- TRIPS_LEGACY_V1
-- Source : schema_2  (2013 – ~2020, lowercase + spaces)
-- Sample : 201306-citibike-tripdata.csv … 201912-citibike-tripdata.csv
-- Columns (15):
--   tripduration | starttime | stoptime
--   start station id | start station name
--   start station latitude | start station longitude
--   end station id | end station name
--   end station latitude | end station longitude
--   bikeid | usertype | birth year | gender
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS CITIBIKE_SYSTEM_DATA.STAGING_NYC.TRIPS_LEGACY_V1 (

    -- trip timing
    TRIPDURATION INT COMMENT 'Trip duration in seconds',
    STARTTIME TIMESTAMP_NTZ COMMENT 'Trip start datetime (no timezone)',
    STOPTIME TIMESTAMP_NTZ COMMENT 'Trip end datetime (no timezone)',
    -- origin station
    START_STATION_ID INT COMMENT 'Numeric start station identifier',
    START_STATION_NAME VARCHAR(255) COMMENT 'Human-readable start station name',
    START_STATION_LATITUDE FLOAT COMMENT 'Start station latitude (WGS84)',
    START_STATION_LONGITUDE FLOAT COMMENT 'Start station longitude (WGS84)',
    -- destination station
    END_STATION_ID INT COMMENT 'Numeric end station identifier',
    END_STATION_NAME VARCHAR(255) COMMENT 'Human-readable end station name',
    END_STATION_LATITUDE FLOAT COMMENT 'End station latitude (WGS84)',
    END_STATION_LONGITUDE FLOAT COMMENT 'End station longitude (WGS84)',
    -- bike & rider
    BIKEID INT COMMENT 'Bike hardware identifier',
    USERTYPE VARCHAR(50) COMMENT 'Subscriber or Customer',
    BIRTH_YEAR INT COMMENT 'Rider self-reported birth year',
    GENDER TINYINT COMMENT '0=unknown, 1=male, 2=female (Citi Bike encoding)',
    -- ingestion metadata
    _SOURCE_FILE VARCHAR(500) COMMENT 'Name of the .csv.gz file this row was loaded from (METADATA$FILENAME)',
    _SOURCE_ROW_NUMBER INT COMMENT 'Row position within the source .csv.gz file (METADATA$FILE_ROW_NUMBER)',
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when this row was staged'
)
COMMENT = 'Citibike NYC legacy trips v1 — schema_2 (lowercase, 2013 - ~2020)'
;

