-- -----------------------------------------------------------
-- TRIPS_MODERN
-- Source : schema_1  (316 files, 2020-present)
-- Sample : 202008-citibike-tripdata_1.csv ... latest
-- Columns (13):
--   ride_id | rideable_type | started_at | ended_at
--   start_station_name | start_station_id
--   end_station_name   | end_station_id
--   start_lat | start_lng | end_lat | end_lng
--   member_casual
-- Note: no bikeid, no usertype demographics -- replaced by member_casual
-- -----------------------------------------------------------
 
CREATE TABLE IF NOT EXISTS CITIBIKE_SYSTEM_DATA.STAGING_NYC.TRIPS_MODERN (
    -- ride identity
    RIDE_ID VARCHAR(50) COMMENT 'Unique ride identifier (UUID-style string)',
    RIDEABLE_TYPE VARCHAR(50) COMMENT 'classic_bike | electric_bike | docked_bike',
    -- trip timing
    STARTED_AT TIMESTAMP_NTZ COMMENT 'Trip start datetime (no timezone)',
    ENDED_AT TIMESTAMP_NTZ COMMENT 'Trip end datetime (no timezone)',
    -- origin station
    START_STATION_NAME VARCHAR(255) COMMENT 'Human-readable start station name',
    START_STATION_ID VARCHAR(50) COMMENT 'Station identifier - alphanumeric in modern schema',
    -- destination station
    END_STATION_NAME VARCHAR(255) COMMENT 'Human-readable end station name',
    END_STATION_ID VARCHAR(50) COMMENT 'Station identifier - alphanumeric in modern schema',
    -- coordinates (stored per-ride, not just per-station)
    START_LAT FLOAT COMMENT 'Start latitude (WGS84)',
    START_LNG FLOAT COMMENT 'Start longitude (WGS84)',
    END_LAT FLOAT COMMENT 'End latitude (WGS84)',
    END_LNG FLOAT COMMENT 'End longitude (WGS84)',
    -- rider segment
    MEMBER_CASUAL VARCHAR(20) COMMENT 'member or casual',
    -- ingestion metadata
    _SOURCE_FILE VARCHAR(500) COMMENT 'Name of the .csv.gz file this row was loaded from (METADATA$FILENAME)',
    _SOURCE_ROW_NUMBER INT COMMENT 'Row position within the source .csv.gz file (METADATA$FILE_ROW_NUMBER)',
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when this row was staged'
)
COMMENT = 'Citibike NYC modern trips - schema_1 (ride_id based, 2020-present)'
;
