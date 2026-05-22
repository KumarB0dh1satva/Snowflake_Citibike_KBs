-- ============================================================
-- RPT_RIDERSHIP_OVER_TIME
-- Monthly ride volume and average duration split by rider type
-- Answers: "How has ridership grown year over year?"
-- ============================================================
 
CREATE OR REPLACE VIEW CITIBIKE_SYSTEM_DATA.RPT_NYC.RPT_RIDERSHIP_OVER_TIME
COMMENT = 'Monthly ridership trends with YoY growth. Grain: 1 row per month per member_casual.'
AS
WITH MONTHLY AS (
    SELECT
        D.YEAR_NUM,
        D.MONTH_NUM,
        D.MONTH_SHORT,
        D.YEAR_MONTH,
        F.MEMBER_CASUAL,
        COUNT(*)                                AS TOTAL_RIDES,
        ROUND(AVG(F.TRIP_DURATION_MIN), 2)      AS AVG_DURATION_MIN,
        ROUND(MEDIAN(F.TRIP_DURATION_MIN), 2)   AS MEDIAN_DURATION_MIN
    FROM CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.FACT_RIDE      F
    JOIN CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_DATES      D
        ON D.DATE_SK = F.START_DATE_SK
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    YEAR_NUM,
    MONTH_NUM,
    MONTH_SHORT,
    YEAR_MONTH,
    MEMBER_CASUAL,
    TOTAL_RIDES,
    AVG_DURATION_MIN,
    MEDIAN_DURATION_MIN,
    -- MoM change
    LAG(TOTAL_RIDES) OVER (
        PARTITION BY MEMBER_CASUAL
        ORDER BY YEAR_MONTH
    )                                                               AS PREV_MONTH_RIDES,
    ROUND(
        (TOTAL_RIDES - LAG(TOTAL_RIDES) OVER (
            PARTITION BY MEMBER_CASUAL ORDER BY YEAR_MONTH)
        ) * 100.0
        / NULLIF(LAG(TOTAL_RIDES) OVER (
            PARTITION BY MEMBER_CASUAL ORDER BY YEAR_MONTH), 0)
    , 2)                                                            AS MOM_GROWTH_PCT,
    -- YoY change
    LAG(TOTAL_RIDES, 12) OVER (
        PARTITION BY MEMBER_CASUAL
        ORDER BY YEAR_MONTH
    )                                                               AS SAME_MONTH_PREV_YEAR,
    ROUND(
        (TOTAL_RIDES - LAG(TOTAL_RIDES, 12) OVER (
            PARTITION BY MEMBER_CASUAL ORDER BY YEAR_MONTH)
        ) * 100.0
        / NULLIF(LAG(TOTAL_RIDES, 12) OVER (
            PARTITION BY MEMBER_CASUAL ORDER BY YEAR_MONTH), 0)
    , 2)                                                            AS YOY_GROWTH_PCT
FROM MONTHLY
ORDER BY YEAR_MONTH, MEMBER_CASUAL;