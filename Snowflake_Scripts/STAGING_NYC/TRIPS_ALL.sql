-- -----------------------------------------------------------
-- TRIPS_ALL
-- Unified NYC trip table (modern + legacy layouts normalized).
-- Populated by SP_LOAD_TRIPS_MODERN / SP_LOAD_TRIPS_LEGACY_V1 / V2.
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS CITIBIKE_SYSTEM_DATA.STAGING_NYC.TRIPS_ALL (
    TRIP_SK NUMBER AUTOINCREMENT PRIMARY KEY,
    -- ride identity
    RIDE_ID VARCHAR(50) COMMENT 'Unique ride identifier (modern only)',
    RIDEABLE_TYPE VARCHAR(50) COMMENT 'classic_bike | electric_bike | docked_bike',
    BIKEID INT COMMENT 'Bike hardware identifier (legacy)',
    -- trip timing
    STARTED_AT TIMESTAMP_NTZ COMMENT 'Trip start datetime (no timezone)',
    ENDED_AT TIMESTAMP_NTZ COMMENT 'Trip end datetime (no timezone)',
    TRIP_DURATION NUMBER(38, 0) COMMENT 'Trip duration in seconds',
    -- origin station
    START_STATION_NAME VARCHAR(255),
    START_STATION_ID VARCHAR(50) COMMENT 'Station id (string; legacy ids cast to varchar)',
    -- destination station
    END_STATION_NAME VARCHAR(255),
    END_STATION_ID VARCHAR(50),
    -- coordinates
    START_LAT FLOAT,
    START_LNG FLOAT,
    END_LAT FLOAT,
    END_LNG FLOAT,
    -- rider segment
    MEMBER_CASUAL VARCHAR(50) COMMENT 'member/casual or Subscriber/Customer',
    BIRTH_YEAR INT,
    GENDER TINYINT COMMENT '0=unknown, 1=male, 2=female',
    -- ingestion metadata
    _SOURCE_FILE VARCHAR(500),
    _SOURCE_ROW_NUMBER INT,
    _LOADED_AT TIMESTAMP_NTZ COMMENT 'Timestamp when row landed in staging table',
    _LOADED_AT_TBL TIMESTAMP_NTZ COMMENT 'Timestamp when row was merged into TRIPS_ALL'
)
COMMENT = 'Citibike NYC combined trips (all schemas)';
