-- -----------------------------------------------------------
-- SP_LOAD_TRIPS_ALL
-- Run all three merge procedures into STAGING_NYC.TRIPS_ALL.
-- Check each child CALL result for row counts.
-- -----------------------------------------------------------

CREATE OR REPLACE PROCEDURE CITIBIKE_SYSTEM_DATA.STAGING_NYC.SP_LOAD_TRIPS_ALL()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    CALL CITIBIKE_SYSTEM_DATA.STAGING_NYC.SP_LOAD_TRIPS_MODERN();
    CALL CITIBIKE_SYSTEM_DATA.STAGING_NYC.SP_LOAD_TRIPS_LEGACY_V1();
    CALL CITIBIKE_SYSTEM_DATA.STAGING_NYC.SP_LOAD_TRIPS_LEGACY_V2();
    RETURN 'SP_LOAD_TRIPS_ALL (NYC): completed MODERN, LEGACY_V1, LEGACY_V2 — see each CALL result for row counts';
END;
$$;
