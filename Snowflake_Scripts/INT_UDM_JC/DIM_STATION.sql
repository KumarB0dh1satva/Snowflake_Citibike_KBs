CREATE TABLE IF NOT EXISTS CITIBIKE_SYSTEM_DATA.INT_UDM_JC.DIM_STATION (
    STATION_SK INT NOT NULL PRIMARY KEY AUTOINCREMENT COMMENT 'Surrogate key',
    STATION_NAME VARCHAR(255) NOT NULL UNIQUE COMMENT 'Canonical station name (TRIM + UPPER normalised)',
    -- Era-specific IDs — both preserved for traceability
    LEGACY_STATION_ID VARCHAR(50) COMMENT 'Numeric ID used in legacy schema (pre-2021)',
    MODERN_STATION_ID VARCHAR(50) COMMENT 'Alphanumeric ID used in modern schema (2021+)',
    -- Authoritative coordinates (MEDIAN across all observed rides)
    STATION_LAT FLOAT COMMENT 'Median latitude across all trip observations',
    STATION_LNG FLOAT COMMENT 'Median longitude across all trip observations',
    -- Data availability flags
    IN_LEGACY BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Station appears in at least one legacy trip',
    IN_MODERN BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Station appears in at least one modern trip',
    AVAILABILITY VARCHAR(15) NOT NULL COMMENT 'BOTH | LEGACY_ONLY | MODERN_ONLY',
    -- Observation stats (for data quality awareness)
    TRIP_OBSERVATIONS INT COMMENT 'Number of trips where this was start or end station',
    LAT_VARIANCE FLOAT COMMENT 'MAX-MIN lat spread across observations — flag if > 0.01',
    LNG_VARIANCE FLOAT COMMENT 'MAX-MIN lng spread across observations — flag if > 0.01',
    -- Ingestion metadata
    _CREATED_AT TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _UPDATED_AT TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Station dimension for Jersey City. Same structure as NYC. Populated by SP_BUILD_DIM_STATION_JC.';