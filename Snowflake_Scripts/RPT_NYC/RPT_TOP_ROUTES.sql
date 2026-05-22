-- ============================================================
-- VIEW 3: RPT_TOP_ROUTES
-- Most travelled station-to-station pairs
-- Answers: "What are the most popular routes?"
-- ============================================================
 
CREATE OR REPLACE VIEW CITIBIKE_SYSTEM_DATA.RPT_NYC.RPT_TOP_ROUTES
COMMENT = 'Top station-to-station routes ranked by trip volume. Grain: 1 row per start/end station pair.'
AS
SELECT
    SS.STATION_NAME                             AS START_STATION,
    ES.STATION_NAME                             AS END_STATION,
    SS.STATION_LAT                              AS START_LAT,
    SS.STATION_LNG                              AS START_LNG,
    ES.STATION_LAT                              AS END_LAT,
    ES.STATION_LNG                              AS END_LNG,
    COUNT(*)                                    AS TOTAL_TRIPS,
    ROUND(AVG(F.TRIP_DURATION_MIN), 2)          AS AVG_DURATION_MIN,
    ROUND(MEDIAN(F.TRIP_DURATION_MIN), 2)       AS MEDIAN_DURATION_MIN,
    -- Round trip detection
    CASE WHEN F.START_STATION_SK = F.END_STATION_SK
         THEN TRUE ELSE FALSE END               AS IS_ROUND_TRIP,
    DENSE_RANK() OVER (
        ORDER BY COUNT(*) DESC
    )                                           AS ROUTE_RANK
FROM CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.FACT_RIDE      F
JOIN CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_STATION    SS
    ON SS.STATION_SK = F.START_STATION_SK
JOIN CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_STATION    ES
    ON ES.STATION_SK = F.END_STATION_SK
WHERE F.START_STATION_SK IS NOT NULL
  AND F.END_STATION_SK   IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 10
ORDER BY TOTAL_TRIPS DESC;