-- -----------------------------------------------------------
-- TRIPS_LEGACY_V2
-- Source : schema_3  (27 files, late-2016 Title Case variant)
-- Sample : 201610-citibike-tripdata_1.csv ... 201612-citibike-tripdata_1.csv
-- Columns (15): identical semantics to V1, Title Case headers in source CSV
--   Trip Duration | Start Time | Stop Time
--   Start Station ID | Start Station Name
--   Start Station Latitude | Start Station Longitude
--   End Station ID | End Station Name
--   End Station Latitude | End Station Longitude
--   Bike ID | User Type | Birth Year | Gender
-- -----------------------------------------------------------
 
CREATE TABLE IF NOT EXISTS CITIBIKE_SYSTEM_DATA.STAGING_JC.TRIPS_LEGACY_V2 ( 
    -- trip timing
    TRIP_DURATION INT COMMENT 'Trip duration in seconds',
    START_TIME TIMESTAMP_NTZ COMMENT 'Trip start datetime (no timezone)',
    STOP_TIME TIMESTAMP_NTZ COMMENT 'Trip end datetime (no timezone)',
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
    BIKE_ID INT COMMENT 'Bike hardware identifier',
    USER_TYPE VARCHAR(50) COMMENT 'Subscriber or Customer',
    BIRTH_YEAR INT COMMENT 'Rider self-reported birth year',
    GENDER TINYINT COMMENT '0=unknown, 1=male, 2=female (Citi Bike encoding)',
    -- ingestion metadata
    _SOURCE_FILE VARCHAR(500) COMMENT 'Name of the .csv.gz file this row was loaded from (METADATA$FILENAME)',
    _SOURCE_ROW_NUMBER INT COMMENT 'Row position within the source .csv.gz file (METADATA$FILE_ROW_NUMBER)',
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when this row was staged'
)
COMMENT = 'Citibike JC legacy trips v2 - schema_3 (Title Case, late-2016 overlap)'
;