CREATE TABLE IF NOT EXISTS CITIBIKE_SYSTEM_DATA.INT_UDM_JC.FACT_RIDE (
        RIDE_SK INT NOT NULL PRIMARY KEY AUTOINCREMENT   COMMENT 'Surrogate key',
    -- Source traceability back to staging
    TRIP_SK_STAGING INT COMMENT 'FK to STAGING_NYC.TRIPS_ALL.TRIP_SK',
    RIDE_ID VARCHAR(50) COMMENT 'UUID-style ID (modern only; NULL for legacy)',
    -- Dimension foreign keys
    START_STATION_SK INT COMMENT 'FK to DIM_STATION.STATION_SK',
    END_STATION_SK INT COMMENT 'FK to DIM_STATION.STATION_SK',
    START_DATE_SK INT COMMENT 'FK to DIM_DATES.DATE_SK (YYYYMMDD)',
    END_DATE_SK INT COMMENT 'FK to DIM_DATES.DATE_SK (YYYYMMDD)',
    -- Trip timing
    STARTED_AT TIMESTAMP_NTZ NOT NULL,
    ENDED_AT TIMESTAMP_NTZ NOT NULL,
    TRIP_DURATION_SEC NUMBER(10,0) NOT NULL COMMENT 'Duration in seconds',
    TRIP_DURATION_MIN NUMBER(10,2) COMMENT 'Duration in minutes (derived)',
    START_HOUR TINYINT COMMENT 'Hour of day 0-23 for time-of-day analysis',
    -- Bike attributes
    RIDEABLE_TYPE VARCHAR(50) COMMENT 'classic_bike | electric_bike | docked_bike',
    BIKEID INT COMMENT 'Legacy only',
    -- Rider segment
    MEMBER_CASUAL VARCHAR(20) NOT NULL COMMENT 'member | casual',
    BIRTH_YEAR INT COMMENT 'Legacy only',
    GENDER TINYINT COMMENT 'Legacy only — 0=unknown 1=male 2=female',
    -- Derived age bucket (legacy only, NULL for modern)
    AGE_AT_RIDE INT COMMENT 'YEAR(STARTED_AT) - BIRTH_YEAR',
    AGE_BUCKET VARCHAR(20) COMMENT '<18 | 18-24 | 25-34 | 35-44 | 45-54 | 55-64 | 65+',
    -- Schema lineage
    SOURCE_SCHEMA VARCHAR(15) NOT NULL COMMENT 'LEGACY_V1 | LEGACY_V2 | MODERN',
    _SOURCE_FILE VARCHAR(500),
    _SOURCE_ROW_NUMBER INT,
    _LOADED_AT TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (START_DATE_SK)
COMMENT = 'Fact table — one row per Jersey City Citi Bike ride.';