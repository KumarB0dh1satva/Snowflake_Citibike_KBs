-- ============================================================
-- RPT_TOP_STATIONS
-- Busiest stations by departures, arrivals and net flow
-- Answers: "Which stations are the most active hubs?"
-- ============================================================
 
CREATE OR REPLACE VIEW CITIBIKE_SYSTEM_DATA.RPT_NYC.RPT_TOP_STATIONS
COMMENT = 'Station activity ranked by departures, arrivals and net bike flow. Grain: 1 row per station.'
AS
WITH DEPARTURES AS (
    SELECT
        START_STATION_SK    AS STATION_SK,
        COUNT(*)            AS TOTAL_DEPARTURES,
        ROUND(AVG(TRIP_DURATION_MIN), 2) AS AVG_DEPARTURE_DURATION_MIN
    FROM CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.FACT_RIDE
    WHERE START_STATION_SK IS NOT NULL
    GROUP BY 1
),
ARRIVALS AS (
    SELECT
        END_STATION_SK      AS STATION_SK,
        COUNT(*)            AS TOTAL_ARRIVALS
    FROM CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.FACT_RIDE
    WHERE END_STATION_SK IS NOT NULL
    GROUP BY 1
)
SELECT
    S.STATION_SK,
    S.STATION_NAME,
    S.STATION_LAT,
    S.STATION_LNG,
    S.AVAILABILITY,
    COALESCE(D.TOTAL_DEPARTURES, 0)                         AS TOTAL_DEPARTURES,
    COALESCE(A.TOTAL_ARRIVALS,   0)                         AS TOTAL_ARRIVALS,
    COALESCE(D.TOTAL_DEPARTURES, 0)
        + COALESCE(A.TOTAL_ARRIVALS, 0)                     AS TOTAL_ACTIVITY,
    COALESCE(D.TOTAL_DEPARTURES, 0)
        - COALESCE(A.TOTAL_ARRIVALS, 0)                     AS NET_FLOW,
    -- Positive = more bikes leaving than arriving (net exporter)
    -- Negative = more bikes arriving than leaving (net importer)
    CASE
        WHEN COALESCE(D.TOTAL_DEPARTURES,0)
           - COALESCE(A.TOTAL_ARRIVALS,0) > 0 THEN 'NET EXPORTER'
        WHEN COALESCE(D.TOTAL_DEPARTURES,0)
           - COALESCE(A.TOTAL_ARRIVALS,0) < 0 THEN 'NET IMPORTER'
        ELSE 'BALANCED'
    END                                                     AS FLOW_TYPE,
    D.AVG_DEPARTURE_DURATION_MIN,
    DENSE_RANK() OVER (ORDER BY COALESCE(D.TOTAL_DEPARTURES,0) DESC) AS DEPARTURE_RANK,
    DENSE_RANK() OVER (ORDER BY COALESCE(A.TOTAL_ARRIVALS,0)   DESC) AS ARRIVAL_RANK,
    DENSE_RANK() OVER (ORDER BY
        COALESCE(D.TOTAL_DEPARTURES,0)
        + COALESCE(A.TOTAL_ARRIVALS,0) DESC)                AS ACTIVITY_RANK
FROM CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_STATION S
LEFT JOIN DEPARTURES D ON D.STATION_SK = S.STATION_SK
LEFT JOIN ARRIVALS   A ON A.STATION_SK = S.STATION_SK
ORDER BY TOTAL_ACTIVITY DESC;